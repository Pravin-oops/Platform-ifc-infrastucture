FROM container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312:2026-08-21-21379287

WORKDIR /build

# Copy only dependency file first for cache reuse
COPY utility/requirement.txt .

# Build wheels
RUN python -m pip install --upgrade pip wheel && \
    pip wheel --wheel-dir=/wheels -r requirement.txt


FROM container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312:2026-08-21-21379287

LABEL application="ifc-bsp-event-processor"
LABEL version="0.0.72"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    IFC_HOME=/app/platform-ifc-infrastructure \
    PYTHONPATH=/app

# Create application user
RUN useradd --create-home \
    --uid 10001 \
    --shell /usr/sbin/nologin \
    connector

WORKDIR /app/platform-ifc-infrastructure

# Install dependencies from wheels
COPY --from=builder /wheels /wheels

RUN pip install --no-index \
    --find-links=/wheels \
    /wheels/*.whl && \
    rm -rf /wheels

# Copy only required source
COPY --chown=connector:connector utility/ utility/
COPY --chown=connector:connector scripts/ scripts/

# Only if actually required by application
COPY --chown=connector:connector .env .env

USER connector

ENTRYPOINT ["python", "scripts/main_ecs.py"]