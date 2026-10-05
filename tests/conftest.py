"""Shared fixtures and helpers.

The entry points in ``scripts/`` are deliberately *not* an importable package -
that is how ``produce_app`` is laid out, and the connector mirrors it. Tests
therefore load them by file path, which has the useful side effect of proving
that the ``sys.path`` bootstrap at the top of each script actually works.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from typing import Iterator

import pytest

#: <repo> - the app root that bundled resource paths such
#: as ``utility/schema.json`` are relative to.
APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: The app root is also the import root: modules import ``utility.*`` directly.
REPO_ROOT = APP_ROOT

SCRIPTS_DIR = os.path.join(APP_ROOT, "scripts")

#: The image definition lives in Docker/, but the build context is APP_ROOT.
DOCKER_DIR = os.path.join(APP_ROOT, "Docker")
DOCKERFILE = os.path.join(DOCKER_DIR, "Dockerfile")
UTILITY_DIR = os.path.join(APP_ROOT, "utility")


def load_script(name: str) -> types.ModuleType:
    """Import ``scripts/<name>.py`` as a standalone module, by path."""
    path = os.path.join(SCRIPTS_DIR, f"{name}.py")
    spec = importlib.util.spec_from_file_location(f"_script_{name}", path)
    assert spec is not None and spec.loader is not None, f"cannot load {path}"

    module = importlib.util.module_from_spec(spec)
    # Registered before execution so a script that imports itself indirectly
    # (or that dataclasses/pickle needs to resolve) finds the same object.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def app_root() -> str:
    return APP_ROOT


@pytest.fixture(scope="session")
def main_script() -> types.ModuleType:
    return load_script("main")


@pytest.fixture(scope="session")
def main_ecs_script() -> types.ModuleType:
    return load_script("main_ecs")


@pytest.fixture
def clean_ifc_env(monkeypatch) -> Iterator[None]:
    """Remove IFC_/APP_CONFIG_PATH values leaked in from the developer's shell.

    The settings loader merges an ``IFC_`` overlay over the YAML, so a stray
    ``IFC_KAFKA__TOPIC`` in the environment would silently change what a test
    is asserting about a config file.
    """
    for key in list(os.environ):
        if key.startswith("IFC_") or key in {"APP_CONFIG_PATH", "IFC_HOME"}:
            monkeypatch.delenv(key, raising=False)
    yield


@pytest.fixture(autouse=True)
def _no_pinned_month() -> Iterator[None]:
    """``load_settings`` pins ``run.month`` for the process; never let it leak
    from one test into the next."""
    from utility import run_gate

    run_gate.set_execution_month(None)
    yield
    run_gate.set_execution_month(None)
