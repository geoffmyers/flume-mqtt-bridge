FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY --from=ha_mqtt_bridge . /opt/ha-mqtt-bridge-toolkit
RUN pip install --no-cache-dir /opt/ha-mqtt-bridge-toolkit
COPY --from=python_github_error_reporter . /opt/python-github-error-reporter
RUN pip install --no-cache-dir /opt/python-github-error-reporter

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# NOTE: the application code (app/) is NOT baked into the image — it is
# bind-mounted at runtime from the repo checkout on the host (see
# docker-compose.yml `volumes:`), so a code change needs only a container
# restart, no rebuild. Rebuild only when app/requirements.txt changes.

RUN useradd --system --uid 1000 --no-create-home --shell /usr/sbin/nologin flume \
    && chown -R flume:flume /app
USER flume

ENTRYPOINT ["python", "/app/main.py"]
