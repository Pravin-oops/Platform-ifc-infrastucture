"""The connector's on-disk shape, which mirrors ``produce_app``.

These are structural tests, not behavioural ones, and that is the point: the
layout is a deliverable in its own right. Two teams now read the same shape -
``scripts/`` for entry points, a flat ``utility/`` for everything they import -
and a subpackage or a stray ``src/`` reintroduced by a later change would break
the symmetry silently. Here it fails a test instead.
"""

from __future__ import annotations

import importlib
import os
import re

import pytest

from tests.conftest import APP_ROOT, DOCKERFILE, SCRIPTS_DIR, UTILITY_DIR

#: Every module in utility/, i.e. what the flattening produced.
UTILITY_MODULES = [
    "audit_utility",
    "auth_helper",
    "connector_config",
    "connector_runner",
    "connector_utility",
    "cyberark_ccp_fetch",
    "error_classifier",
    "failure_catalog",
    "failure_notifier",
    "health_utility",
    "kafka_factory",
    "kafka_preflight",
    "kafka_publisher",
    "kafka_serializers",
    "observability_utility",
    "recon_gate",
    "sequence_allocator",
    "resilience_utility",
    "run_gate",
    "schema_registry_client",
    "tb_outcome_schema",
    "trigger_batch_notifier",
    "trigger_definitions",
    "trigger_payload",
    "trigger_source",
]

ENTRY_POINTS = ["main", "main_ecs"]

#: Non-code assets that live beside the modules, exactly as produce_app keeps
#: its YAML, its .json schema and its requirements.txt inside utility/.
UTILITY_ASSETS = [
    "requirements.txt",
    "connector_config.yaml",
    "schema.json",
]


#: This file is the only legitimate place the old names appear - they are the
#: patterns being searched for - so the scanners skip themselves.
SCANNER = os.path.abspath(__file__)


def python_files_under(root: str):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in {"__pycache__", ".pytest_cache"}]
        for name in filenames:
            path = os.path.join(dirpath, name)
            if name.endswith(".py") and os.path.abspath(path) != SCANNER:
                yield path


class TestDirectoryShape:
    def test_the_app_has_only_the_produce_app_code_directories(self):
        """scripts/ and utility/ hold the code; src/, config/ and schemas/ are gone."""
        for legacy in ("src", "config", "schemas"):
            assert not os.path.isdir(os.path.join(APP_ROOT, legacy)), (
                f"{legacy}/ should have been folded into scripts/ + utility/"
            )

        assert os.path.isdir(SCRIPTS_DIR)
        assert os.path.isdir(UTILITY_DIR)

    def test_utility_is_flat(self):
        """produce_app's utility/ has no subpackages, and neither does this one."""
        subdirs = [
            name
            for name in os.listdir(UTILITY_DIR)
            if os.path.isdir(os.path.join(UTILITY_DIR, name)) and name != "__pycache__"
        ]
        assert subdirs == [], f"utility/ must stay flat, found {subdirs}"

    def test_scripts_holds_exactly_the_entry_points(self):
        found = sorted(
            os.path.splitext(name)[0]
            for name in os.listdir(SCRIPTS_DIR)
            if name.endswith(".py")
        )
        assert found == sorted(ENTRY_POINTS)

    def test_utility_holds_exactly_the_expected_modules(self):
        found = sorted(
            os.path.splitext(name)[0]
            for name in os.listdir(UTILITY_DIR)
            if name.endswith(".py") and name != "__init__.py"
        )
        assert found == sorted(UTILITY_MODULES)

    def test_the_package_marker_is_spelled_correctly(self):
        """produce_app's ``__init__py`` is a known bug; it is not reproduced here."""
        assert os.path.isfile(os.path.join(UTILITY_DIR, "__init__.py"))
        assert not os.path.exists(os.path.join(UTILITY_DIR, "__init__py"))

    @pytest.mark.parametrize("asset", UTILITY_ASSETS)
    def test_asset_sits_beside_the_modules(self, asset):
        path = os.path.join(UTILITY_DIR, asset)
        assert os.path.isfile(path), f"utility/{asset} is missing"
        assert os.path.getsize(path) > 0, f"utility/{asset} is empty"


class TestImports:
    @pytest.mark.parametrize("module", UTILITY_MODULES)
    def test_module_imports(self, module):
        assert importlib.import_module(f"utility.{module}") is not None

    @pytest.mark.parametrize("script", ENTRY_POINTS)
    def test_entry_point_loads_and_exposes_main(self, script):
        from tests.conftest import load_script

        module = load_script(script)
        assert callable(getattr(module, "main", None)), f"{script}.py must expose main()"

    def test_no_module_still_references_the_old_package(self):
        """The flattening is complete only if nothing imports ``ifc_connector``."""
        stale = re.compile(r"(?<!ifc_trigger_)\bifc_connector\b")
        offenders = []

        for path in python_files_under(APP_ROOT):
            text = open(path, "r", encoding="utf-8").read()
            if stale.search(text):
                offenders.append(os.path.relpath(path, APP_ROOT))

        assert offenders == [], f"stale ifc_connector references in {offenders}"

    def test_no_module_imports_a_sibling_by_a_subpackage_path(self):
        """``utility.kafka.publisher`` and friends must not come back."""
        bad = re.compile(
            r"from utility\.(auth|kafka|failures|triggers)\."
        )
        offenders = [
            os.path.relpath(path, APP_ROOT)
            for path in python_files_under(APP_ROOT)
            if bad.search(open(path, "r", encoding="utf-8").read())
        ]
        assert offenders == [], f"subpackage-style imports in {offenders}"


class TestRequirements:
    def test_requirements_lists_the_runtime_dependencies(self):
        text = open(os.path.join(UTILITY_DIR, "requirements.txt"), "r", encoding="utf-8").read()
        for package in ("boto3", "PyYAML", "requests", "fastavro", "confluent-kafka", "pydantic"):
            assert package in text, f"{package} missing from utility/requirements.txt"

    def test_the_bsp_client_is_an_active_requirement(self):
        """Proprietary to Barclays, but pip-installable from the internal index.

        It is therefore an ordinary requirement, not a wheel dropped into a
        vendor/ directory - so it must be a live line here, not a commented one.
        """
        text = open(os.path.join(UTILITY_DIR, "requirements.txt"), "r", encoding="utf-8").read()
        active = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        assert any(line.startswith("bsp_python_client") for line in active), (
            "bsp_python_client must be an uncommented requirement"
        )


class TestDockerfile:
    """The image has to agree with the layout, or the container starts and dies."""

    def dockerfile(self) -> str:
        return open(DOCKERFILE, "r", encoding="utf-8").read()

    def test_it_lives_in_the_docker_directory(self):
        assert os.path.isfile(DOCKERFILE), "the Dockerfile belongs in Docker/"
        assert not os.path.exists(os.path.join(APP_ROOT, "Dockerfile")), (
            "the Dockerfile moved into Docker/; a copy left at the app root would drift"
        )

    def test_the_build_context_is_documented_as_the_app_root(self):
        """COPY reaches into utility/ and scripts/, so Docker/ cannot be the context.

        Getting this wrong fails as 'COPY failed: file not found', which reads
        like a missing file rather than a wrong -f/context pairing.
        """
        text = self.dockerfile()
        assert "docker build -f Docker/Dockerfile" in text

    def test_it_installs_requirements_from_utility(self):
        assert "COPY utility/requirements.txt" in self.dockerfile()

    def test_the_bsp_client_has_no_vendor_special_case(self):
        """It pip-installs from the Barclays internal index like anything else.

        The old build dropped a wheel into vendor/ and fell back to a
        BSP_INDEX_URL build arg. Both are gone; a reintroduced COPY vendor/
        would silently resurrect a directory that no longer exists.
        """
        text = self.dockerfile()
        assert "COPY vendor" not in text
        assert "/build/vendor" not in text
        assert "BSP_INDEX_URL" not in text
        assert not os.path.exists(os.path.join(APP_ROOT, "vendor"))

    def test_it_copies_the_new_directories(self):
        text = self.dockerfile()
        assert "utility/ /app/ifc_trigger_connector/utility/" in text
        assert "scripts/ /app/ifc_trigger_connector/scripts/" in text
        assert "src/" not in text

    def test_it_copies_no_directory_the_repository_does_not_have(self):
        """A COPY of a missing source directory fails the whole image build."""
        sources = re.findall(r"^COPY\s+(?:--\S+\s+)*(\S+)", self.dockerfile(), re.MULTILINE)
        for source in sources:
            assert os.path.exists(os.path.join(APP_ROOT, source)), f"COPY {source}: not in the repository"

    def test_pythonpath_and_ifc_home_agree_with_the_package_root(self):
        text = self.dockerfile()
        # Modules import utility.* directly, so both point at the app root.
        assert "PYTHONPATH=/app/ifc_trigger_connector" in text
        assert "IFC_HOME=/app/ifc_trigger_connector" in text

    def test_the_entrypoint_is_the_ecs_script(self):
        text = self.dockerfile()
        assert "scripts/main_ecs.py" in text
        assert "python\", \"-m\", \"ifc_connector" not in text
