"""BSP client configuration and BAM token lifecycle.

The BSP Python client owns the librdkafka SASL/OAUTHBEARER wiring, including the
``oauth_cb`` that refreshes the broker token. This module wraps it so that:

* the token returned by ``get_token`` is normalised (it comes back variously as
  a string, a ``(token, expiry)`` tuple, or a dict) and shape-checked before it
  is handed to the Schema Registry;
* the token's ``exp`` claim is tracked, so a long run refreshes ahead of
  expiry instead of failing a batch mid-flight - the one place where the ECS
  runtime genuinely differs from a 15-minute Lambda;
* failures classify onto the catalogue.
"""

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

#: Used when the JWT carries no usable exp claim. BAM tokens are short-lived;
#: an hour is conservative and still avoids re-authenticating per message.
DEFAULT_TOKEN_LIFETIME_SECONDS = 3600


def normalize_token(raw: Any) -> str:
    """Coerce whatever ``get_token`` returned into a bare JWT string."""
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
            # Prefix only - never log a whole bearer token.
            context={"token_prefix": token[:24]},
        )

    return token


def token_expiry(token: str) -> Optional[float]:
    """Read the ``exp`` claim without verifying the signature.

    Verification is the broker's and the registry's job; all that is needed here
    is to know when to ask for a new one.
    """
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)  # restore base64url padding
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except (IndexError, ValueError, binascii.Error, UnicodeDecodeError):
        logger.debug("Could not decode JWT claims; falling back to a fixed lifetime")
        return None

    exp = claims.get("exp")
    return float(exp) if isinstance(exp, (int, float)) else None


@runtime_checkable
class TokenProvider(Protocol):
    """What a consumer of a bearer token actually needs.

    ``BSPTokenProvider`` is the real implementation, but the local path uses a
    stand-in that raises, and the tests use a fixed-token fake. Typing on the
    behaviour rather than the class keeps all three legitimate without anyone
    subclassing something they do not want the machinery of.
    """

    @property
    def seconds_remaining(self) -> float: ...

    def get(self, *, force_refresh: bool = False) -> str: ...


class BSPTokenProvider:
    """Caches and refreshes the BAM token used for the Schema Registry.

    The broker's own token is refreshed by librdkafka through the BSP client's
    ``oauth_cb``; this provider covers the Schema Registry REST calls, which sit
    outside that callback.
    """

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

        logger.info(
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
    """Thin wrapper over ``bsp_python_client.auth.bsp_authenticator``.

    Imported lazily so the package can be installed, tested and reasoned about
    on a machine that has no access to the internal BSP wheel.
    """

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
        """librdkafka producer properties, with the OAUTHBEARER callback wired in."""
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

        # Copied, never handed out: the caller may add overrides, and the cached
        # BSP config has to stay the same for the next call.
        config = dict(cached)
        if overrides:
            config.update(overrides)
        return config

    def token_provider(self, *, refresh_margin_seconds: int = 300) -> BSPTokenProvider:
        def factory() -> Any:
            return self._auth().get_token(self.producer_config())

        return BSPTokenProvider(factory, refresh_margin_seconds=refresh_margin_seconds)