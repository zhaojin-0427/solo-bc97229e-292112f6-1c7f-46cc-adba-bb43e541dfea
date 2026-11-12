# syntax=docker/dockerfile:1

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data \
    MAX_UPLOAD_BYTES=17179869184 \
    JOB_WORKERS=1 \
    KEEP_RELEASES=2 \
    PROCESS_STALE_SECONDS=30 \
    HEARTBEAT_INTERVAL=2 \
    REAP_INTERVAL=10 \
    STAGING_ORPHAN_SECONDS=3600

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py ingest.py publish.py ownership.py /app/
COPY templates /app/templates
COPY static /app/static
COPY examples /app/examples
COPY tools /app/tools

RUN useradd --create-home --uid 10001 --user-group app \
    && mkdir -p /data \
    && chown -R app:app /data /app
USER app

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/', timeout=3).read(1)"

# One gunicorn worker owns the in-process job executor. Threads handle
# concurrent uploads and status requests without leaving the single writer.
CMD ["gunicorn", "--workers=1", "--worker-class=gthread", "--threads=8", "--timeout=0", "--graceful-timeout=30", "--access-logfile=-", "--error-logfile=-", "--bind=0.0.0.0:5000", "app:app"]
