# syntax=docker/dockerfile:1
# RHEL 8 image for the IFC trigger connector.
#
# The base image already provides Python 3.12 on PATH as `python`, plus the
# system CA bundle, so there is no interpreter install and no yum step.
#
# Build context is the repository root, not Docker/. It has to be: the COPYs
# below reach into utility/ and scripts/, and Docker refuses to copy from outside
# the context. Pass the file with -f and the context as the final argument:
#
#     docker build -f Docker/Dockerfile --secret id=ca_root,src=/path/to/CARoot.pem \
#         -t ifc-trigger-connector:0.0.4 .
#
# The --secret is required: see the CA step below.
#
# .dockerignore stays at the context root, which is where both the classic
# builder and BuildKit look for it by default.
#
# Build speed: layers run from least to most frequently changed. A commit that
# only touches code reuses the cached dependency layer, as long as the CI job
# can see a previous image to take cache from (e.g. --cache-from on the last
# pushed tag) - without that every CI build starts cold.

ARG BASE_IMAGE=container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312:2026-08-21-21379287

FROM ${BASE_IMAGE}

LABEL application="ifc-bsp-event-processor"
LABEL version="0.0.4"

# PYTHONPATH is the app root: modules import `utility.*` directly. The entry
# points also put their own app root on sys.path, so this matters only for
# `python -c` / `python -m` one-offs inside the container.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    IFC_HOME=/app/ifc_trigger_connector \
    PYTHONPATH=/app/ifc_trigger_connector

# Rarely changes, so it sits first and stays cached.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin connector

WORKDIR /app/ifc_trigger_connector

# Dependencies before code: this layer is rebuilt only when requirements.txt
# changes, not on every commit.
#
# bsp_python_client is proprietary to Barclays and served from the internal
# package index rather than PyPI. It is an ordinary line in requirements.txt and
# installs like every other dependency - there is no vendored wheel.
#
# tzdata is in requirements.txt deliberately: utility/run_gate.py resolves the
# weekday in Europe/London through zoneinfo, and the image may carry no system
# zone database.
COPY utility/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

COPY --chown=connector:connector utility/ /app/ifc_trigger_connector/utility/
COPY --chown=connector:connector scripts/ /app/ifc_trigger_connector/scripts/

# Barclays root CA (CARoot.pem), which the Schema Registry's certificate chains
# to. It is never committed: the build takes it as a BuildKit secret, mounted
# for this one step only, so it is in neither the repository nor the build
# context. It is added to the system trust store, so
# /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem - the schema_registry and
# cyberark ca paths - carries it alongside the base image's own CAs. The build
# fails without the secret: in SECURE mode an image without it cannot fetch a
# schema id. Runs as root, so it sits before USER below.
RUN --mount=type=secret,id=ca_root,required=true \
    cp /run/secrets/ca_root /etc/pki/ca-trust/source/anchors/barclays-ca-root.pem \
    && update-ca-trust extract

USER connector

EXPOSE 8080

# Liveness only: a task that is draining is deliberately not ready, but must
# stay alive until the drain completes.
# Probed with the interpreter, so the image needs no curl (and no yum step).
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/live', timeout=3).read()"]

# main_ecs.py is the platform entry point: it reads APP_CONFIG_PATH from the task
# definition and maps the run summary onto the exit code. main.py is the same
# pipeline with a CLI in front, for `docker run ... catalogue` and one-off runs.
ENTRYPOINT ["python", "/app/ifc_trigger_connector/scripts/main_ecs.py"]
