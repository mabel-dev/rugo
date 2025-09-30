"""
Tests for version synchronization between pyproject.toml and __init__.py
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_version_exists():
    """Test that __version__ is defined."""
    import rugo
    
    assert hasattr(rugo, "__version__")
    assert isinstance(rugo.__version__, str)
    assert len(rugo.__version__) > 0


def test_version_format():
    """Test that version follows semantic versioning format."""
    import rugo
    
    # Should be in format like "0.1.1" or "0.1.1-dev"
    parts = rugo.__version__.split("-")[0].split(".")
    assert len(parts) >= 2, f"Version should have at least major.minor: {rugo.__version__}"
    
    # Check that major and minor are numbers
    assert parts[0].isdigit(), f"Major version should be a number: {parts[0]}"
    assert parts[1].isdigit(), f"Minor version should be a number: {parts[1]}"


def test_version_matches_pyproject():
    """Test that __version__ matches the version in pyproject.toml."""
    import rugo
    import tomllib
    
    # Read version from pyproject.toml
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    
    # Use tomllib (Python 3.11+) or fallback
    try:
        with open(pyproject_path, "rb") as f:
            pyproject_data = tomllib.load(f)
    except (ImportError, AttributeError):
        # Fallback for Python < 3.11
        import re
        with open(pyproject_path, "r") as f:
            content = f.read()
            match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
            if match:
                pyproject_version = match.group(1)
            else:
                pytest.skip("Could not parse version from pyproject.toml")
                return
    else:
        pyproject_version = pyproject_data["project"]["version"]
    
    # Compare versions
    assert rugo.__version__ == pyproject_version, (
        f"Version mismatch: rugo.__version__={rugo.__version__} "
        f"but pyproject.toml has version={pyproject_version}"
    )
