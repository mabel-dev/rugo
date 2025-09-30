#!/usr/bin/env python3
"""
Simple script to verify version synchronization works correctly.
"""

import sys
from pathlib import Path

# Add rugo to path for testing
sys.path.insert(0, str(Path(__file__).parent))

import rugo

print("=" * 60)
print("Version Synchronization Verification")
print("=" * 60)
print()

# Check version
print(f"rugo.__version__ = {rugo.__version__!r}")
print()

# Read version from pyproject.toml
try:
    import tomllib
    pyproject_path = Path(__file__).parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject_data = tomllib.load(f)
    pyproject_version = pyproject_data["project"]["version"]
except (ImportError, AttributeError):
    # Fallback for Python < 3.11
    import re
    pyproject_path = Path(__file__).parent / "pyproject.toml"
    with open(pyproject_path, "r") as f:
        content = f.read()
        match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
        if match:
            pyproject_version = match.group(1)
        else:
            pyproject_version = "Could not parse"

print(f"pyproject.toml version = {pyproject_version!r}")
print()

# Check if they match
if rugo.__version__ == pyproject_version:
    print("✅ SUCCESS: Versions are in sync!")
else:
    print(f"❌ ERROR: Versions do not match!")
    print(f"   Expected: {pyproject_version}")
    print(f"   Got: {rugo.__version__}")
    sys.exit(1)

print()
print("=" * 60)
print("How it works:")
print("=" * 60)
print()
print("1. pyproject.toml is the single source of truth for version")
print("2. rugo/__init__.py uses importlib.metadata.version() to read")
print("   the version from package metadata when installed")
print("3. Falls back to hardcoded version (same as pyproject.toml)")
print("   when package is not installed (development mode)")
print()
print("To keep versions in sync:")
print("- Update version ONLY in pyproject.toml")
print("- Update fallback version in rugo/__init__.py to match")
print()
