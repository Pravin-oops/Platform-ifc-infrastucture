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
# The Required-by lines show which package pulled each one in.
# Red Hat's own RPM copies of those packages are removed first: pip will not
# uninstall an RPM-owned package, so an old RPM copy would otherwise stay in
# /usr/lib*/python3.12 beside the upgraded one and keep the scan finding open.
ARG REMOVE_PYTHON_RPMS="python3.12-urllib3 python3.12-cryptography python3.12-jwt python3.12-pyjwt python3.12-certifi"
COPY utility/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    set -eu; \
    for p in ${REMOVE_PYTHON_RPMS}; do \
        if rpm -q "$p" >/dev/null 2>&1; then echo "Removing RPM copy: $(rpm -q "$p")"; rpm -e --nodeps --noscripts "$p"; fi; \
    done; \
    grep -v '# security' /tmp/requirements.txt > /tmp/base-requirements.txt; \
    pip install -r /tmp/base-requirements.txt; \
    security="$(grep '# security' /tmp/requirements.txt | sed 's/#.*//' | xargs)"; \
    if [ -n "$security" ]; then echo "Enforcing: $security"; pip install --upgrade $security; fi; \
    pip check || echo "WARNING: bsp_python_client pins overridden for the security fixes above"; \
    for spec in $security; do \
        pip show "$(echo "$spec" | sed -E 's/[<>=!~ ;].*//')" 2>/dev/null | grep -E '^(Name|Version|Required-by):' || true; \
    done; \
    rm -f /tmp/requirements.txt /tmp/base-requirements.txt

# ---------------------------------------------------------------------------
# Strip the builder image down to what the app needs at runtime.
#
# The base is a *builder* image: compilers, debuggers, container and .NET
# tooling and desktop libraries that the app never uses but that the image
# scanner reports. Every dependency ships as a pre-built wheel, so none of this
# is needed once the pip install above has finished.
#
# 1. dnf remove: build, debug, container, .NET and desktop packages, plus
#    anything that only they needed. If dnf refuses the whole list because a
#    package is still needed by dnf itself, it falls back to one at a time and
#    keeps the blocked ones.
# 2. dnf upgrade: apply every OS update the repos have (openssl, pcre2, expat,
#    python3.12, util-linux, libstdc++, ca-certificates, ...). Needs repo access
#    during the build; --build-arg OS_UPGRADE=false skips it.
# 3. Delete tools shipped outside RPM: CLIs (az, aws, helm, terraform, kubectl,
#    oc, dotnet, pwsh), every Go binary (Go crypto/tls CVEs), and every copy of pip
#    including the bundled pip wheels (pip is only needed at build time).
# 4. rpm -e --nodeps: the package manager itself (dnf/yum, libcomps), the system
#    Python it runs on (3.6 on RHEL 8, 3.9 on RHEL 9), and the libraries only it
#    uses (rpm-build-libs, libgomp, curl, glib2). --build-arg
#    STRIP_PKG_MANAGER=false skips this if anything layered on top needs dnf.
# Packages that are not installed are skipped, so a new base tag will not
# break the build. The final check fails the build if a removal took out
# something the app needs.
# ---------------------------------------------------------------------------
ARG REMOVE_PACKAGES="\
    gcc gcc-c++ cpp gcc-gdb-plugin gcc-plugin-annobin libstdc++-devel \
    gdb gdb-headless gdb-gdbserver \
    runc skopeo criu fuse-overlayfs slirp4netns libslirp containers-common container-selinux git-lfs \
    crun podman buildah conmon netavark aardvark-dns passt \
    emacs-filesystem libX11 libX11-common libX11-xcb libXi libXrender librsvg2 libgs libsoup perl-URI \
    openssh openssh-clients \
    pcre2-devel pcre2-utf16 pcre2-utf32 python3.12-devel python3.12-pip \
    azure-cli helm terraform kubectl openshift-clients vim-enhanced vim-common"
# RPM name patterns (rpm -qa globs) removed in step 1 as well.
ARG REMOVE_PATTERNS="dotnet* aspnetcore* netstandard-targeting-pack* powershell*"
# Tools the builder image may ship as plain binaries or folders rather than RPMs.
ARG REMOVE_BINARIES="az aws aws_completer helm terraform kubectl oc dotnet pwsh"
ARG REMOVE_DIRS="/opt/az /usr/lib64/az /root/.azure /usr/local/aws-cli \
    /usr/lib64/dotnet /usr/share/dotnet /usr/local/share/dotnet /opt/microsoft /root/.dotnet /root/.nuget"
ARG REMOVE_PKG_MANAGER_PACKAGES="\
    dnf dnf-data yum dnf-plugins-core libdnf librepo libsolv libmodulemd libcomps \
    rpm-build-libs rpm-sign-libs libgomp \
    curl curl-minimal libcurl libcurl-minimal glib2 openssl \
    python3 python3.12-pip-wheel"
ARG STRIP_PKG_MANAGER=true
ARG OS_UPGRADE=true

RUN set -eu; \
    installed() { for p in "$@"; do if rpm -q "$p" >/dev/null 2>&1; then echo "$p"; fi; done; }; \
    set -f; \
    pkgs="$(installed ${REMOVE_PACKAGES}) $(rpm -qa --qf '%{NAME}\n' ${REMOVE_PATTERNS} 2>/dev/null || true)"; \
    set +f; \
    pkgs="$(echo $pkgs)"; \
    if [ -n "$pkgs" ]; then \
        echo "Removing: $pkgs"; \
        if ! dnf -y --disablerepo='*' remove $pkgs; then \
            echo "Bulk removal refused by dnf; removing packages one at a time"; \
            for p in $pkgs; do \
                if rpm -q "$p" >/dev/null 2>&1; then \
                    if dnf -y -q --disablerepo='*' remove "$p" >/dev/null 2>&1; then echo "  removed $p"; \
                    else echo "  kept $p (dnf still needs it; step 4 may remove it)"; fi; \
                fi; \
            done; \
        fi; \
    fi; \
    if [ "${OS_UPGRADE}" = "true" ]; then \
        echo "Applying OS updates"; \
        dnf -y upgrade --refresh; \
    fi; \
    rm -rf /var/cache/dnf /var/cache/yum; \
    for b in ${REMOVE_BINARIES}; do \
        for d in $(echo "$PATH" | tr ':' ' '); do \
            if [ -e "$d/$b" ] || [ -L "$d/$b" ]; then echo "Removing binary: $d/$b"; rm -f "$d/$b"; fi; \
        done; \
    done; \
    rm -rf ${REMOVE_DIRS}; \
    for d in /usr/bin /usr/sbin /usr/local /usr/libexec /opt /root /home; do \
        if [ -d "$d" ]; then find "$d" -xdev -type f -perm /111 -size +1M -exec grep -l -a -F 'Go buildinf:' {} + 2>/dev/null || true; fi; \
    done | while read -r f; do echo "Removing Go binary: $f"; rm -f "$f"; done; \
    find / -xdev -type d -name 'pip-[0-9]*.dist-info' 2>/dev/null | while read -r d; do \
        echo "Removing pip: $d"; rm -rf "$(dirname "$d")/pip" "$d"; \
    done; \
    find / -xdev -type f -name 'pip-[0-9]*.whl' 2>/dev/null | while read -r f; do \
        echo "Removing pip wheel: $f"; rm -f "$f"; \
    done; \
    for d in $(echo "$PATH" | tr ':' ' '); do rm -f "$d/pip" "$d/pip3" "$d"/pip3.*; done; \
    if [ "${STRIP_PKG_MANAGER}" = "true" ]; then \
        pm="$(installed ${REMOVE_PKG_MANAGER_PACKAGES}) \
            $(rpm -qa --qf '%{NAME}\n' | grep -E '^(platform-python|python3-)' || true)"; \
        pm="$(echo $pm)"; \
        if [ -n "$pm" ]; then \
            echo "Removing (no deps): $pm"; \
            rpm -e --nodeps --noscripts $pm; \
        fi; \
    fi; \
    echo "Copies of the security-pinned Python packages left in the image:"; \
    find / -xdev -type d \( -iname 'urllib3-*.*-info' -o -iname 'cryptography-*.*-info' \
        -o -iname 'pyjwt-*.*-info' -o -iname 'certifi-*.*-info' \) 2>/dev/null | sed 's/^/  /' || true; \
    python3.12 -c "import importlib.util, ssl, decimal, zoneinfo, http.server, xml.etree.ElementTree, \
        boto3, botocore, bsp_python_client, certifi, confluent_kafka, cryptography, dotenv, fastavro, jwt, \
        pydantic, requests, urllib3, yaml; \
        assert importlib.util.find_spec('pip') is None, 'pip is still installed'; \
        print('runtime check OK:', ssl.OPENSSL_VERSION, '| PyJWT', jwt.__version__, '| certifi', certifi.__version__, \
              '| cryptography', cryptography.__version__, '| urllib3', urllib3.__version__)"

COPY --chown=connector:connector .env .env
COPY --chown=connector:connector scripts/ scripts/
COPY --chown=connector:connector utility/ utility/

# Changes every release, so it sits last to keep the layers above cached.
ARG APP_VERSION=0.0.72
LABEL application="ifc-bsp-event-processor" \
      version="${APP_VERSION}"

USER connector

EXPOSE 8080

# python3.12 explicitly: the unversioned python/python3 links belong to the
# system Python, which the cleanup above removes.
ENTRYPOINT ["python3.12", "scripts/main_ecs.py"]
