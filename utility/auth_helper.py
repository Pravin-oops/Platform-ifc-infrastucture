from __future__ import annotations

import base64
import binascii
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable

from utility import failure_catalog as catalog
from utility.error_classifier import PreflightError

logger = logging.getLogger(__name__)

DEFAULT_TOKEN_LIFETIME_SECONDS = 3600


def normalize_token(raw: Any) -> str:
    if isinstance(raw, tuple):
        token = raw[0] if raw else None
    elif isinstance(raw, dict):
        token = raw.get("token") or raw.get("access_token") or raw.get("id_token")
    else:
        token = raw

    if not token:
        raise PreflightError(
            "BAM returned an empty token",
            catalog.AUTHENTICATION_FAILURE,
            context={"raw_type": type(raw).__name__},
        )

    token = str(token).strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()

    if token.count(".") != 2:
        raise PreflightError(
            f"Malformed JWT from BAM: expected 3 segments, got {token.count('.') + 1}",
            catalog.AUTHENTICATION_FAILURE,
            context={"token_prefix": token[:24]},
        )

    return token


def token_expiry(token: str) -> Optional[float]:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except (IndexError, ValueError, binascii.Error, UnicodeDecodeError):
        logger.debug("Could not decode JWT claims; falling back to a fixed lifetime")
        return None

    exp = claims.get("exp")
    return float(exp) if isinstance(exp, (int, float)) else None


@runtime_checkable
class TokenProvider(Protocol):
    @property
    def seconds_remaining(self) -> float: ...

    def get(self, *, force_refresh: bool = False) -> str: ...


class BSPTokenProvider:
    def __init__(
        self,
        token_factory: Callable[[], Any],
        *,
        refresh_margin_seconds: int = 300,
    ):
        self._factory = token_factory
        self._margin = refresh_margin_seconds
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at: float = 0.0

    def _fetch(self) -> str:
        try:
            token = normalize_token(self._factory())
        except PreflightError:
            raise
        except Exception as exc:
            raise PreflightError(
                f"BAM token acquisition failed: {exc}",
                catalog.AUTHENTICATION_FAILURE,
                cause=exc,
            ) from exc

        exp = token_expiry(token)
        self._expires_at = exp if exp else time.time() + DEFAULT_TOKEN_LIFETIME_SECONDS
        self._token = token

        logger.debug(
            "BAM token acquired",
            extra={"expires_in_seconds": round(self._expires_at - time.time())},
        )
        return token

    def get(self, *, force_refresh: bool = False) -> str:
        with self._lock:
            expired = time.time() >= (self._expires_at - self._margin)
            if force_refresh or self._token is None or expired:
                return self._fetch()
            return self._token

    @property
    def seconds_remaining(self) -> float:
        return max(0.0, self._expires_at - time.time())


class BSPClient:
    def __init__(self, config_path: str, *, authenticator: Any = None):
        self._config_path = config_path
        self._authenticator = authenticator
        self._kafka_config: Optional[Dict[str, Any]] = None

    def _auth(self) -> Any:
        if self._authenticator is None:
            try:
                from bsp_python_client.auth.bsp_authenticator import BSPAuthenticator
            except ImportError as exc:
                raise PreflightError(
                    "bsp_python_client is not installed; it is proprietary to Barclays and "
                    "resolves from the internal package index, so an image built without "
                    "access to that index will not have it",
                    catalog.CONTAINER_FAILURE,
                    cause=exc,
                ) from exc
            self._authenticator = BSPAuthenticator()
        return self._authenticator

    def producer_config(self, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        cached = self._kafka_config
        if cached is None:
            try:
                cached = self._auth().get_config(self._config_path, {}, "producer")
            except Exception as exc:
                raise PreflightError(
                    f"Failed to build the BSP producer config from {self._config_path}: {exc}",
                    catalog.AUTHENTICATION_FAILURE,
                    context={"bsp_config_path": self._config_path},
                    cause=exc,
                ) from exc
            self._kafka_config = cached

        config = dict(cached)
        if overrides:
            config.update(overrides)
        return config

    def token_provider(self, *, refresh_margin_seconds: int = 300) -> BSPTokenProvider:
        def factory() -> Any:
            return self._auth().get_token(self.producer_config())

        return BSPTokenProvider(factory, refresh_margin_seconds=refresh_margin_seconds)