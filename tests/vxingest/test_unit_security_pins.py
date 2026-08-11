import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_pyproject_pins_safe_msgpack_and_setuptools_versions():
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())

    dependencies = pyproject["project"]["dependencies"]
    uv_constraints = pyproject["tool"]["uv"]["constraint-dependencies"]

    assert "msgpack>=1.2.1" in dependencies
    assert "setuptools>=78.1.1" in uv_constraints


def test_ingest_dockerfile_upgrades_vulnerable_packages():
    dockerfile = (REPO_ROOT / "docker" / "ingest" / "Dockerfile").read_text()

    assert "RUN /app/.venv/bin/pip install --no-cache-dir --upgrade \\" in dockerfile
    assert '"msgpack>=1.2.1"' in dockerfile
    assert '"setuptools>=78.1.1"' in dockerfile
