"""
Raw PDF storage shared by the producer and all workers.

Primary: S3 (works when workers run on different machines).
Fallback: a local directory (only valid when all workers share the same disk).
Message payloads only carry the blob URI, never the PDF bytes, so Redis
messages stay small whatever the document size.
"""

import os
import logging
import threading
import tempfile

from ingest import config

logger = logging.getLogger(__name__)

_CACHE_DIR = os.path.join(tempfile.gettempdir(), "dji_ingest_cache")
_download_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _s3():
    from tools.extraction import s3_client, S3_BUCKET_NAME
    return s3_client, S3_BUCKET_NAME


def put_pdf(job_id: str, filename: str, data: bytes) -> str:
    """Store the uploaded PDF and return its URI (s3://... or file://...)."""
    key = f"{config.RAW_PDF_S3_PREFIX}{job_id}.pdf"
    try:
        client, bucket = _s3()
        client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/pdf")
        return f"s3://{bucket}/{key}"
    except Exception as e:
        logger.warning(f"S3 upload of raw PDF failed ({e}); falling back to local blob dir")
        os.makedirs(config.LOCAL_BLOB_DIR, exist_ok=True)
        path = os.path.join(config.LOCAL_BLOB_DIR, f"{job_id}.pdf")
        with open(path, "wb") as f:
            f.write(data)
        return f"file://{path}"


def local_pdf_path(job_id: str, uri: str) -> str:
    """
    Return a local file path for the job's PDF, downloading it at most once per
    worker process (each page task of the same job reuses the cached copy).
    """
    if uri.startswith("file://"):
        return uri[len("file://"):]

    os.makedirs(_CACHE_DIR, exist_ok=True)
    path = os.path.join(_CACHE_DIR, f"{job_id}.pdf")
    with _locks_guard:
        lock = _download_locks.setdefault(job_id, threading.Lock())
    with lock:
        if not os.path.exists(path):
            bucket, key = uri[len("s3://"):].split("/", 1)
            client, _ = _s3()
            tmp = path + ".part"
            client.download_file(bucket, key, tmp)
            os.replace(tmp, path)  # atomic: other threads never see a partial file
    return path


def delete_pdf(job_id: str, uri: str):
    """Remove the raw PDF once the job is finished (best effort)."""
    try:
        if uri.startswith("s3://"):
            bucket, key = uri[len("s3://"):].split("/", 1)
            client, _ = _s3()
            client.delete_object(Bucket=bucket, Key=key)
        elif uri.startswith("file://"):
            os.remove(uri[len("file://"):])
    except Exception as e:
        logger.warning(f"Could not delete raw PDF for job {job_id}: {e}")


def drop_local_cache(job_id: str):
    try:
        os.remove(os.path.join(_CACHE_DIR, f"{job_id}.pdf"))
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug(f"cache cleanup failed for {job_id}: {e}")
