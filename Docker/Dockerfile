# docker build -f Docker/Dockerfile --build-arg APP_VERSION=0.0.72 -t ifc-bsp-event-processor:0.0.72 .
# Requires BuildKit (default in Docker 23+) for the pip cache mount.

ARG BASE_IMAGE=container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312:2026-08-21-21379287
FROM ${BASE_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    IFC_HOME=/app/platform-ifc-infrastructure

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin connector

WORKDIR /app/platform-ifc-infrastructure

# Dependency layer: re-runs only when requirements.txt changes; pip's download
# cache persists across builds via the cache mount and never lands in the image.
COPY utility/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /tmp/requirements.txt

COPY --chown=connector:connector .env .env
COPY --chown=connector:connector scripts/ scripts/
COPY --chown=connector:connector utility/ utility/

# Changes every release, so it sits last to keep the layers above cached.
ARG APP_VERSION=0.0.72
LABEL application="ifc-bsp-event-processor" \
      version="${APP_VERSION}"

USER connector

EXPOSE 8080

ENTRYPOINT ["python", "scripts/main_ecs.py"]
