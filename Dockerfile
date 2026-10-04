FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/models
WORKDIR /srv

COPY requirements.lock requirements-acceleration.txt ./
ARG INSTALL_ACCELERATION=false
RUN pip install --no-cache-dir -r requirements.lock \
    && if [ "$INSTALL_ACCELERATION" = "true" ]; then pip install --no-cache-dir -r requirements-acceleration.txt; fi \
    && groupadd --gid 10001 gateway \
    && useradd --uid 10001 --gid gateway --no-create-home --shell /usr/sbin/nologin gateway \
    && mkdir -p /models \
    && chown gateway:gateway /models

COPY --chown=gateway:gateway app ./app
COPY --chown=gateway:gateway migrations ./migrations
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --start-period=45s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2)"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers", "--timeout-keep-alive", "5", "--timeout-graceful-shutdown", "30"]
