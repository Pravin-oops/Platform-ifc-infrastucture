# RHEL 8 image for the IFC trigger connector.
#
# Based on the version built and pushed to ECR from the Barclays environment, with
# three corrections marked FIXED below. The base image already provides Python
# 3.12 on PATH as `python`, so there is no interpreter install and no apt-get -
# only ca-certificates and curl come from yum.
#
# Build context is the *connector* directory, not Docker/. It has to be: every
# COPY below reaches into utility/, scripts/, samples/ and data/, and Docker
# refuses to copy from outside the context. Pass the file with -f and the context
# as the final argument.
#
#   from the repository root
#     docker build -f ifc_trigger_connector/Docker/Dockerfile -t ifc-trigger-connector:0.0.4 ifc_trigger_connector/
#
#   from inside ifc_trigger_connector/
#     docker build -f Docker/Dockerfile -t ifc-trigger-connector:0.0.4 .
#
# .dockerignore stays at the context root (ifc_trigger_connector/.dockerignore),
# which is where both the classic builder and BuildKit look for it by default.

ARG BASE_IMAGE=container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312:2026-08-21-21379287


# ---------------------------------------------------------------------------
# Build stage - wheels are built here so the runtime image carries no toolchain
# ---------------------------------------------------------------------------
FROM ${BASE_IMAGE} AS build

LABEL application="ifc-bsp-event-processor"
LABEL version="0.0.4"

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# FIXED: the file in the repository is requirements.txt (plural). A COPY of
# requirement.txt fails the build outright, so this has to match the repo.
COPY utility/requirements.txt .

# bsp_python_client is proprietary to Barclays and served from the internal
# package index rather than PyPI. It is an ordinary line in requirements.txt and
# installs like every other dependency - there is no vendored wheel.
#
# tzdata is in requirements.txt deliberately: utility/run_gate.py resolves the
# weekday in Europe/London through zoneinfo, and a slim RHEL image may carry no
# system zone database. Without it the gate silently falls back to UTC.
RUN python -m pip install --upgrade pip wheel \
    && pip wheel --wheel-dir /wheels -r requirements.txt


# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM ${BASE_IMAGE} AS runtime

LABEL application="ifc-bsp-event-processor"
LABEL version="0.0.4"

# FIXED: the app directory must be named ifc_trigger_connector.
#
# Every module imports `ifc_trigger_connector.utility.*`, so with /app on
# PYTHONPATH the package directory has to carry exactly that name. Copying to
# /app/platform-ifc-infrastructure makes the entry point die immediately with
#
#     ModuleNotFoundError: No module named 'ifc_trigger_connector'
#
# and a hyphenated name can never be a Python package in any case. The image is
# still labelled and tagged for the platform-ifc-infrastructure application - the
# directory name is an import path, not the deployment name.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    IFC_HOME=/app/ifc_trigger_connector \
    PYTHONPATH=/app

# curl backs the HEALTHCHECK below. If this ever moves to a minimal base where
# curl is absent, drop it and probe with the interpreter instead:
#   CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/live', timeout=3).read()"]
#
# update-ca-trust rebuilds /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem,
# which is the path utility/csm_aws_fetch.py and the CSM config already expect.
RUN yum install -y ca-certificates curl \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && (update-ca-trust extract || true) \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin connector

WORKDIR /app/ifc_trigger_connector

COPY --from=build /wheels /wheels
RUN python -m pip install --upgrade pip \
    && pip install --no-index --find-links /wheels /wheels/*.whl \
    && rm -rf /wheels

COPY --chown=connector:connector utility/ /app/ifc_trigger_connector/utility/
COPY --chown=connector:connector scripts/ /app/ifc_trigger_connector/scripts/
COPY --chown=connector:connector samples/ /app/ifc_trigger_connector/samples/

# FIXED: data/ was missing. connector_config_ecs_local.yaml reads `data` as the
# source while the Trigger Event BDP is unbuilt; without this COPY that config
# finds nothing and the run reports ZERO_RECORDS. Drop this line once the
# connector reads the real source.
COPY --chown=connector:connector data/ /app/ifc_trigger_connector/data/

# The Barclays root CA is mounted or baked in per environment; the app searches
# the candidate paths in utility/csm_aws_fetch.py and falls back to the system
# store. On RHEL the supported route is the anchors directory plus a re-extract,
# rather than overwriting the extracted bundle (which the next update-ca-trust
# run would undo):
#
#   COPY certs/barclays-root-ca.pem /etc/pki/ca-trust/source/anchors/
#   RUN update-ca-trust extract

USER connector

EXPOSE 8080

# Liveness only. Readiness is probed by the load balancer or the ECS service
# health check, because a task that is draining is deliberately not ready.
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health/live || exit 1

# main_ecs.py is the platform entry point: it reads APP_CONFIG_PATH from the task
# definition and maps the run summary onto the exit code. main.py is the same
# pipeline with a CLI in front, for `docker run ... catalogue` and one-off runs.
ENTRYPOINT ["python", "/app/ifc_trigger_connector/scripts/main_ecs.py"]
