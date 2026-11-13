ARG BASE_IMAGE=container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312:2026-08-21-21379287
FROM ${BASE_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    IFC_HOME=/app/platform-ifc-infrastructure

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin connector

WORKDIR /app/platform-ifc-infrastructure

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

ARG REMOVE_PACKAGES="\
    gcc gcc-c++ cpp gcc-gdb-plugin gcc-plugin-annobin libstdc++-devel \
    gdb gdb-headless gdb-gdbserver \
    runc skopeo criu fuse-overlayfs slirp4netns libslirp containers-common container-selinux git-lfs \
    crun podman buildah conmon netavark aardvark-dns passt \
    emacs-filesystem libX11 libX11-common libX11-xcb libXi libXrender librsvg2 libgs libsoup perl-URI \
    openssh openssh-clients \
    pcre2-devel pcre2-utf16 pcre2-utf32 python3.12-devel python3.12-pip \
    azure-cli helm terraform kubectl openshift-clients vim-enhanced vim-common"
ARG REMOVE_PATTERNS="dotnet* aspnetcore* netstandard-targeting-pack* powershell*"
ARG REMOVE_BINARIES="az aws aws_completer helm terraform kubectl oc dotnet pwsh"
ARG REMOVE_DIRS="/opt/az /usr/lib64/az /root/.azure /usr/local/aws-cli \
    /usr/lib64/dotnet /usr/share/dotnet /usr/local/share/dotnet /opt/microsoft /root/.dotnet /root/.nuget"
ARG REMOVE_PKG_MANAGER_PACKAGES="\
    dnf dnf-data yum dnf-plugins-core libdnf librepo libsolv libmodulemd libcomps \
    rpm-build-libs rpm-sign-libs libgomp \
    curl curl-minimal libcurl libcurl-minimal glib2 openssl \
    python3 python3.12-pip-wheel"
ARG REMOVE_UNUSED_OS_LIBS="\
    libxml2 util-linux util-linux-core libfdisk libsmartcols libmount libblkid libuuid"
ARG STRIP_PKG_MANAGER=true
ARG OS_UPGRADE=true
ARG DNF_RELEASEVER=""
ARG DNF_EXTRA_OPTS=""

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
        echo "dnf releasever: $(cat /etc/dnf/vars/releasever 2>/dev/null || echo 'not pinned')"; \
        echo "dnf repositories (id / metadata date):"; \
        dnf -q repolist -v 2>/dev/null | grep -E '^Repo-(id|updated)' | sed 's/^/  /' || true; \
        echo "Applying OS updates ${DNF_RELEASEVER:+(releasever=$DNF_RELEASEVER) }${DNF_EXTRA_OPTS}"; \
        dnf -y upgrade --refresh ${DNF_RELEASEVER:+--releasever=$DNF_RELEASEVER} ${DNF_EXTRA_OPTS}; \
        echo "Core OS package versions now in the image:"; \
        rpm -q glibc openssl-libs expat libgcc libstdc++ xz-libs | sed 's/^/  /' || true; \
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
    libs="$(installed ${REMOVE_UNUSED_OS_LIBS})"; \
    libs="$(echo $libs)"; \
    if [ -n "$libs" ]; then echo "Removing unused OS libraries (no deps): $libs"; rpm -e --nodeps --noscripts $libs; fi; \
    find / -xdev -type f -perm /6000 2>/dev/null | while read -r f; do echo "Dropping setuid/setgid: $f"; chmod u-s,g-s "$f"; done; \
    echo "Copies of the security-pinned Python packages left in the image:"; \
    find / -xdev -type d \( -iname 'urllib3-*.*-info' -o -iname 'cryptography-*.*-info' \
        -o -iname 'pyjwt-*.*-info' -o -iname 'certifi-*.*-info' \) 2>/dev/null | sed 's/^/  /' || true; \
    python3.12 -c "import importlib.util, ssl, decimal, zoneinfo, http.server, xml.etree.ElementTree, \
        boto3, botocore, bsp_python_client, certifi, confluent_kafka, cryptography, dotenv, fastavro, jwt, \
        pydantic, requests, urllib3, uuid, yaml; \
        assert importlib.util.find_spec('pip') is None, 'pip is still installed'; \
        uuid.uuid4(); xml.etree.ElementTree.fromstring('<ok/>'); \
        print('runtime check OK:', ssl.OPENSSL_VERSION, '| PyJWT', jwt.__version__, '| certifi', certifi.__version__, \
              '| cryptography', cryptography.__version__, '| urllib3', urllib3.__version__)"

COPY --chown=connector:connector .env .env
COPY --chown=connector:connector scripts/ scripts/
COPY --chown=connector:connector utility/ utility/

LABEL application="ifc-bsp-event-processor"
LABEL version="0.0.72"

USER connector

EXPOSE 8080

ENTRYPOINT ["python3.12", "scripts/main_ecs.py"]
