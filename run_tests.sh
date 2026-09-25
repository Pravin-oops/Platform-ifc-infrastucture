#!/usr/bin/env bash
#
# Test runner for the IFC trigger connector (Linux / macOS / Git Bash).
#
#   ./run_tests.sh                 unit tests + offline smoke checks
#   ./run_tests.sh --cov           also produce a coverage report
#   ./run_tests.sh --no-install    skip dependency installation
#   ./run_tests.sh -k envelope     pass anything else straight to pytest
#
# Nothing here touches AWS, BSP, CSM, BAM or a Kafka broker. Every check runs
# against the bundled schemas, so it is safe to run on a
# laptop and in CI without credentials.

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$APP_DIR"
VENV_DIR="${IFC_VENV:-$APP_DIR/.venv}"

INSTALL=1
COVERAGE=0
PYTEST_ARGS=()

for arg in "$@"; do
    case "$arg" in
        --no-install) INSTALL=0 ;;
        --cov)        COVERAGE=1 ;;
        *)            PYTEST_ARGS+=("$arg") ;;
    esac
done

say() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# ---------------------------------------------------------------------------
# Interpreter
# ---------------------------------------------------------------------------

if [[ -x "$VENV_DIR/bin/python" ]]; then
    PY="$VENV_DIR/bin/python"
elif [[ -x "$VENV_DIR/Scripts/python.exe" ]]; then
    PY="$VENV_DIR/Scripts/python.exe"      # Git Bash on Windows
elif [[ $INSTALL -eq 1 ]]; then
    say "Creating virtualenv at $VENV_DIR"
    "${PYTHON:-python3}" -m venv "$VENV_DIR"
    PY="$VENV_DIR/bin/python"
    [[ -x "$PY" ]] || PY="$VENV_DIR/Scripts/python.exe"
else
    PY="${PYTHON:-python3}"
fi

say "Interpreter"
"$PY" --version

# ---------------------------------------------------------------------------
# Dependencies
#
# These install an explicit list rather than requirements.txt: the BSP client
# resolves only from the Barclays internal index, and confluent-kafka is only
# needed to talk to a real broker - neither is required by these tests.
# ---------------------------------------------------------------------------

if [[ $INSTALL -eq 1 ]]; then
    say "Installing test dependencies"
    "$PY" -m pip install --quiet --upgrade pip
    "$PY" -m pip install --quiet pytest pytest-cov PyYAML fastavro boto3 moto pydantic requests python-dotenv tzdata
fi

# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

cd "$APP_DIR"

# Under Git Bash the interpreter is a native Windows build, so PYTHONPATH needs
# Windows paths and ';' as the separator - a POSIX '/c/...' entry is silently
# ignored and every utility import then fails.
case "$(uname -s 2>/dev/null || echo unknown)" in
    MINGW*|MSYS*|CYGWIN*)
        PATH_SEP=';'
        command -v cygpath > /dev/null && REPO_ROOT="$(cygpath -w "$REPO_ROOT")"
        ;;
    *)
        PATH_SEP=':'
        ;;
esac

export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+${PATH_SEP}${PYTHONPATH}}"

say "Unit tests"
if [[ $COVERAGE -eq 1 ]]; then
    "$PY" -m pytest -q --cov --cov-report=term-missing "${PYTEST_ARGS[@]}"
else
    "$PY" -m pytest -q "${PYTEST_ARGS[@]}"
fi

# ---------------------------------------------------------------------------
# Smoke checks: the two entry points, on the paths that need no network.
# ---------------------------------------------------------------------------

say "Smoke: failure catalogue (scripts/main.py catalogue)"
"$PY" scripts/main.py catalogue > /dev/null
echo "catalogue rendered as JSON"

say "Smoke: ECS entry point refuses to start without a config"
# Blank APP_CONFIG_PATH explicitly: otherwise main_ecs.py would load it from the
# tracked .env (load_dotenv never overrides a variable that is already set).
if APP_CONFIG_PATH= "$PY" scripts/main_ecs.py > /dev/null 2>&1; then
    echo "FAIL: main_ecs.py exited 0 with no APP_CONFIG_PATH" >&2
    exit 1
fi
echo "main_ecs.py exited non-zero as expected"

printf '\n\033[1;32mAll checks passed.\033[0m\n'
