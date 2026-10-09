# docker build -f Docker/Dockerfile -t ifc-trigger-connector:0.0.4 .

ARG BASE_IMAGE=container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312:2026-08-21-21379287

FROM ${BASE_IMAGE}

LABEL application="ifc-bsp-event-processor"
LABEL version="0.0.4"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    IFC_HOME=/app/ifc_trigger_connector \
    PYTHONPATH=/app/ifc_trigger_connector

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin connector

WORKDIR /app/ifc_trigger_connector

COPY utility/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

COPY --chown=connector:connector utility/ /app/ifc_trigger_connector/utility/
COPY --chown=connector:connector scripts/ /app/ifc_trigger_connector/scripts/

USER connector

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/live', timeout=3).read()"]

ENTRYPOINT ["python", "/app/ifc_trigger_connector/scripts/main_ecs.py"]
