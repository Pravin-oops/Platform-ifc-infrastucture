"""Path helpers that treat ``s3://`` URIs and local paths interchangeably.

Every read here is streaming or single-object; nothing loads a whole prefix into
memory. That is deliberate - the OOM failure scenario is caused as often by an
eager loader as by a genuine leak.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict, Iterator, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_S3_SCHEME = "s3://"
_s3_client = None


def s3() -> Any:
    """Lazily created S3 client, so importing this module needs no credentials."""
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
    """Raised when the Trigger BDP cannot be read or written.

    Maps to the TBB catalogue entries 'Trigger BDP Read Failure' and 'Trigger
    BDP Write Failure' so the classifier can route the incident correctly.
    """

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


def iter_object_paths(path: str, suffixes: Tuple[str, ...] | list[str]) -> Iterator[str]:
    """Yield object paths under ``path``, sorted, without materialising the list.

    Accepts a single object, a local directory or an S3 prefix.
    """
    wanted = tuple(s.lower() for s in suffixes)

    if is_s3_path(path):
        bucket, prefix = parse_s3_path(path)
        if prefix.lower().endswith(wanted):
            yield path
            return

        paginator = s3().get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                # Keys arrive lexicographically ordered within and across pages,
                # so the stream is already deterministic - no buffering needed.
                for item in page.get("Contents", []):
                    key = item["Key"]
                    if key.endswith("/") or not key.lower().endswith(wanted):
                        continue
                    yield f"s3://{bucket}/{key}"
        except ClientError as exc:
            raise SourceAccessError(
                f"S3 list failed for {path}: {exc.response.get('Error', {}).get('Code')}",
                path=path,
                operation="list",
                cause=exc,
            ) from exc
        return

    if os.path.isfile(path):
        if path.lower().endswith(wanted):
            yield path
        return

    if os.path.isdir(path):
        for name in sorted(os.listdir(path)):
            if name.lower().endswith(wanted):
                yield os.path.join(path, name)
        return

    raise SourceAccessError(f"Path not found: {path}", path=path, operation="list")


def copy_object(source: str, destination: str) -> None:
    """Copy one object, S3-to-S3 server side where possible."""
    if is_s3_path(source) and is_s3_path(destination):
        src_bucket, src_key = parse_s3_path(source)
        dst_bucket, dst_key = parse_s3_path(destination)
        try:
            s3().copy_object(
                Bucket=dst_bucket,
                Key=dst_key,
                CopySource={"Bucket": src_bucket, "Key": src_key},
            )
        except ClientError as exc:
            raise SourceAccessError(
                f"S3 copy failed {source} -> {destination}",
                path=destination,
                operation="write",
                cause=exc,
            ) from exc
        return

    write_bytes(destination, read_text(source).encode("utf-8"))


def join_path(base: str, *parts: str) -> str:
    """Join a base prefix/directory with path parts, S3-safe."""
    if is_s3_path(base):
        return base.rstrip("/") + "/" + "/".join(p.strip("/") for p in parts if p)
    return os.path.join(base, *parts)


def materialise_local(path: str, suffix: str = ".yaml") -> str:
    """Return a local filesystem path for ``path``.

    The BSP client insists on a real file, so an S3-hosted client config is
    written to a temp file first.
    """
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
    """Resolve an app-relative resource (e.g. a bundled schema) inside the image.

    Paths are relative to the app root (the directory holding ``utility/``), so a bundled
    schema is named ``utility/schema.json``. ``IFC_HOME`` is set in the
    Dockerfile; it falls back to the app root, two levels up from this file
    (``<app>/utility/connector_utility.py``), when running from a checkout.
    """
    if is_s3_path(relative) or os.path.isabs(relative):
        return relative

    root = os.environ.get("IFC_HOME") or os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
    return os.path.join(root, relative)


def load_schema_document(path: str) -> Dict[str, Any]:
    """Load an Avro schema from a repo-relative path, absolute path or S3 URI."""
    return read_json(package_resource(path))
