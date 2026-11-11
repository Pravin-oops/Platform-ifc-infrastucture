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
    pip install -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

# ---------------------------------------------------------------------------
# Strip the builder image down to what the app needs at runtime.
#
# The base is a *builder* image: compilers, debuggers, container tooling and
# desktop libraries that the app never uses but that the image scanner reports.
# Every dependency ships as a pre-built wheel, so none of this is needed once
# the pip install above has finished.
#
# Step 1 (dnf remove): build, debug, container and desktop packages, plus
#   anything that only they needed.
# Step 2 (rpm -e --nodeps): the package manager itself (dnf/yum), the system
#   Python 3.6 it runs on, and the libraries only it uses (curl, glib2).
#   Set --build-arg STRIP_PKG_MANAGER=false if anything layered on top of this
#   image still needs to run dnf.
# Packages that are not installed are skipped, so a new base tag will not
# break the build. The final import check fails the build if a removal took
# out something the app needs.
# ---------------------------------------------------------------------------
ARG REMOVE_PACKAGES="\
    gcc gcc-c++ cpp gcc-gdb-plugin gcc-plugin-annobin libstdc++-devel libgomp \
    gdb gdb-headless gdb-gdbserver \
    runc skopeo criu fuse-overlayfs slirp4netns libslirp containers-common container-selinux git-lfs \
    emacs-filesystem libXi libXrender librsvg2 libgs libsoup perl-URI openssh openssh-clients \
    pcre2-devel pcre2-utf16 pcre2-utf32 python3.12-devel python3.12-pip"
ARG REMOVE_PKG_MANAGER_PACKAGES="\
    dnf dnf-data yum dnf-plugins-core libdnf librepo libsolv libmodulemd \
    curl curl-minimal libcurl libcurl-minimal glib2 openssl"
ARG STRIP_PKG_MANAGER=true

RUN set -eu; \
    installed() { for p in "$@"; do if rpm -q "$p" >/dev/null 2>&1; then echo "$p"; fi; done; }; \
    pkgs="$(installed ${REMOVE_PACKAGES})"; \
    if [ -n "$pkgs" ]; then \
        echo "Removing: $pkgs"; \
        dnf -y --disablerepo='*' remove $pkgs; \
    fi; \
    rm -rf /var/cache/dnf /var/cache/yum; \
    if [ "${STRIP_PKG_MANAGER}" = "true" ]; then \
        pm="$(installed ${REMOVE_PKG_MANAGER_PACKAGES}) \
            $(rpm -qa --qf '%{NAME}\n' | grep -E '^(platform-python|python3-)' || true)"; \
        pm="$(echo $pm)"; \
        if [ -n "$pm" ]; then \
            echo "Removing (no deps): $pm"; \
            rpm -e --nodeps --noscripts $pm; \
        fi; \
    fi; \
    python -c "import ssl, decimal, zoneinfo, http.server, xml.etree.ElementTree, \
        boto3, botocore, bsp_python_client, confluent_kafka, dotenv, fastavro, pydantic, requests, yaml; \
        print('runtime check OK:', ssl.OPENSSL_VERSION)"

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
