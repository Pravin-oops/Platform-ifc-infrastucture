from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_S3_SCHEME = "s3://"
_s3_client = None


def s3() -> Any:
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def is_s3_path(path: str) -> bool:
    return str(path).startswith(_S3_SCHEME)


def parse_s3_path(path: str) -> Tuple[str, str]:
    parsed = urlparse(str(path))
    return parsed.netloc, parsed.path.lstrip("/")


class SourceAccessError(RuntimeError):
    def __init__(self, message: str, *, path: str, operation: str, cause: Optional[BaseException] = None):
        super().__init__(message)
        self.path = path
        self.operation = operation
        self.cause = cause


def read_text(path: str, encoding: str = "utf-8") -> str:
    if is_s3_path(path):
        bucket, key = parse_s3_path(path)
        try:
            obj = s3().get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            raise SourceAccessError(
                f"S3 read failed for {path}: {exc.response.get('Error', {}).get('Code')}",
                path=path,
                operation="read",
                cause=exc,
            ) from exc
        return obj["Body"].read().decode(encoding)

    try:
        with open(path, "r", encoding=encoding) as handle:
            return handle.read()
    except OSError as exc:
        raise SourceAccessError(f"Local read failed for {path}: {exc}", path=path, operation="read", cause=exc) from exc


def read_json(path: str) -> Any:
    return json.loads(read_text(path))


def write_bytes(path: str, data: bytes, *, content_type: str = "application/octet-stream") -> str:
    if is_s3_path(path):
        bucket, key = parse_s3_path(path)
        try:
            s3().put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        except ClientError as exc:
            raise SourceAccessError(
                f"S3 write failed for {path}: {exc.response.get('Error', {}).get('Code')}",
                path=path,
                operation="write",
                cause=exc,
            ) from exc
        return path

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        with open(path, "wb") as handle:
            handle.write(data)
    except OSError as exc:
        raise SourceAccessError(f"Local write failed for {path}: {exc}", path=path, operation="write", cause=exc) from exc
    return path


def write_json(path: str, document: Any) -> str:
    return write_bytes(
        path,
        json.dumps(document, indent=2, default=str).encode("utf-8"),
        content_type="application/json",
    )


def join_path(base: str, *parts: str) -> str:
    if is_s3_path(base):
        return base.rstrip("/") + "/" + "/".join(p.strip("/") for p in parts if p)
    return os.path.join(base, *parts)


def materialise_local(path: str, suffix: str = ".yaml") -> str:
    if not is_s3_path(path):
        return path

    handle = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    try:
        handle.write(read_text(path).encode("utf-8"))
    finally:
        handle.close()
    logger.debug("Materialised %s to %s", path, handle.name)
    return handle.name


def package_resource(relative: str) -> str:
    if is_s3_path(relative) or os.path.isabs(relative):
        return relative

    root = os.environ.get("IFC_HOME") or os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
    return os.path.join(root, relative)


def load_schema_document(path: str) -> Dict[str, Any]:
    return read_json(package_resource(path))
