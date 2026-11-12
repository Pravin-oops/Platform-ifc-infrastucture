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
# Lines tagged "# security" in requirements.txt are installed as an upgrade
# *after* the rest, so a pin inside bsp_python_client cannot block the fix: pip
# prints a dependency warning (shown by `pip check`) and installs it anyway.
COPY utility/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    set -eu; \
    grep -v '# security' /tmp/requirements.txt > /tmp/base-requirements.txt; \
    pip install -r /tmp/base-requirements.txt; \
    security="$(grep '# security' /tmp/requirements.txt | sed 's/#.*//' | xargs)"; \
    if [ -n "$security" ]; then echo "Enforcing: $security"; pip install --upgrade $security; fi; \
    pip check || echo "WARNING: bsp_python_client pins overridden for the security fixes above"; \
    rm -f /tmp/requirements.txt /tmp/base-requirements.txt

# ---------------------------------------------------------------------------
# Strip the builder image down to what the app needs at runtime.
#
# The base is a *builder* image: compilers, debuggers, container tooling and
# desktop libraries that the app never uses but that the image scanner reports.
# Every dependency ships as a pre-built wheel, so none of this is needed once
# the pip install above has finished.
#
# Step 1 (dnf remove): build, debug, container and desktop packages, plus
#   anything that only they needed. If dnf refuses the whole list because a
#   package is still needed by dnf itself, it falls back to one at a time and
#   keeps the blocked ones.
# Step 2 (rpm -e --nodeps): the package manager itself (dnf/yum), the system
#   Python it runs on (3.6 on RHEL 8, 3.9 on RHEL 9), and the libraries only it
#   uses (rpm-build-libs, libgomp, curl, glib2).
#   Set --build-arg STRIP_PKG_MANAGER=false if anything layered on top of this
#   image still needs to run dnf.
# Before step 1, UPDATE_PACKAGES (ca-certificates) is updated from the repos.
# After step 1, every Go binary is deleted (the app is pure Python); bundled
# CLIs such as helm/oc/terraform carry Go crypto/tls CVEs.
# Packages that are not installed are skipped, so a new base tag will not
# break the build. The final import check fails the build if a removal took
# out something the app needs.
# ---------------------------------------------------------------------------
ARG REMOVE_PACKAGES="\
    gcc gcc-c++ cpp gcc-gdb-plugin gcc-plugin-annobin libstdc++-devel \
    gdb gdb-headless gdb-gdbserver \
    runc skopeo criu fuse-overlayfs slirp4netns libslirp containers-common container-selinux git-lfs \
    crun podman buildah conmon netavark aardvark-dns passt \
    emacs-filesystem libXi libXrender librsvg2 libgs libsoup perl-URI openssh openssh-clients \
    pcre2-devel pcre2-utf16 pcre2-utf32 python3.12-devel python3.12-pip \
    azure-cli helm terraform kubectl openshift-clients vim-enhanced vim-common"
# CLI tools the builder image may ship as plain binaries rather than RPMs.
ARG REMOVE_BINARIES="az helm terraform kubectl oc"
ARG REMOVE_PKG_MANAGER_PACKAGES="\
    dnf dnf-data yum dnf-plugins-core libdnf librepo libsolv libmodulemd \
    rpm-build-libs rpm-sign-libs libgomp \
    curl curl-minimal libcurl libcurl-minimal glib2 openssl"
ARG STRIP_PKG_MANAGER=true
# OS packages to update from the Red Hat repos before dnf is removed (needs repo
# access during the build). Set to "" to skip.
ARG UPDATE_PACKAGES="ca-certificates"

RUN set -eu; \
    installed() { for p in "$@"; do if rpm -q "$p" >/dev/null 2>&1; then echo "$p"; fi; done; }; \
    if [ -n "${UPDATE_PACKAGES}" ]; then \
        echo "Updating: ${UPDATE_PACKAGES}"; \
        dnf -y upgrade --refresh ${UPDATE_PACKAGES}; \
        rpm -q ${UPDATE_PACKAGES}; \
    fi; \
    pkgs="$(installed ${REMOVE_PACKAGES})"; \
    if [ -n "$pkgs" ]; then \
        echo "Removing: $pkgs"; \
        if ! dnf -y --disablerepo='*' remove $pkgs; then \
            echo "Bulk removal refused by dnf; removing packages one at a time"; \
            for p in $pkgs; do \
                if rpm -q "$p" >/dev/null 2>&1; then \
                    if dnf -y -q --disablerepo='*' remove "$p" >/dev/null 2>&1; then echo "  removed $p"; \
                    else echo "  kept $p (dnf still needs it; step 2 may remove it)"; fi; \
                fi; \
            done; \
        fi; \
    fi; \
    rm -rf /var/cache/dnf /var/cache/yum; \
    for b in ${REMOVE_BINARIES}; do \
        for d in $(echo "$PATH" | tr ':' ' '); do \
            if [ -e "$d/$b" ] || [ -L "$d/$b" ]; then echo "Removing binary: $d/$b"; rm -f "$d/$b"; fi; \
        done; \
    done; \
    rm -rf /opt/az /usr/lib64/az /root/.azure; \
    for d in /usr/bin /usr/sbin /usr/local /usr/libexec /opt /root /home; do \
        if [ -d "$d" ]; then find "$d" -xdev -type f -perm /111 -size +1M -exec grep -l -a -F 'Go buildinf:' {} + 2>/dev/null || true; fi; \
    done | while read -r f; do echo "Removing Go binary: $f"; rm -f "$f"; done; \
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
        boto3, botocore, bsp_python_client, certifi, confluent_kafka, dotenv, fastavro, jwt, pydantic, requests, yaml; \
        print('runtime check OK:', ssl.OPENSSL_VERSION, '| PyJWT', jwt.__version__, '| certifi', certifi.__version__)"

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
