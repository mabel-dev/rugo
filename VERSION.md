# Version Management

This document explains how version numbers are managed in the Rugo project.

## Single Source of Truth

The project uses **`pyproject.toml`** as the single source of truth for version numbers, following modern Python packaging standards (PEP 621).

## How It Works

### 1. Version Declaration
The version is declared in `pyproject.toml`:

```toml
[project]
name = "rugo"
version = "0.1.1"
```

### 2. Runtime Version Access
The `rugo/__init__.py` file dynamically reads the version from package metadata:

```python
try:
    from importlib.metadata import version
    __version__ = version("rugo")
except Exception:
    # Fallback version for development/editable installs
    __version__ = "0.1.1"
```

This approach:
- ✅ Uses package metadata when rugo is installed
- ✅ Falls back to a hardcoded version during development
- ✅ Eliminates the need to manually update multiple files

## Updating the Version

When you need to update the version:

1. **Update `pyproject.toml`**:
   ```toml
   version = "0.2.0"  # New version
   ```

2. **Update the fallback in `rugo/__init__.py`**:
   ```python
   except Exception:
       __version__ = "0.2.0"  # Match pyproject.toml
   ```

3. **Verify the sync**:
   ```bash
   make verify-version
   # or
   python verify_version.py
   ```

4. **Run tests**:
   ```bash
   python -m pytest tests/test_version.py -v
   ```

## Automated Verification

### Makefile Target
Use the `verify-version` target to check version synchronization:

```bash
make verify-version
```

### Test Suite
The test suite includes version synchronization checks:

```bash
python -m pytest tests/test_version.py -v
```

This ensures that:
- The version exists and is properly formatted
- The version in `__init__.py` matches `pyproject.toml`

## Benefits

1. **Single Source of Truth**: Version is defined once in `pyproject.toml`
2. **Automatic in Production**: Installed packages read from metadata
3. **Works in Development**: Fallback version for editable installs
4. **Verified**: Tests ensure versions stay in sync
5. **Standard Compliant**: Follows PEP 621 and modern Python packaging

## Technical Details

### importlib.metadata
- Available in Python 3.8+ (this project requires 3.9+)
- Reads version from package metadata after installation
- Returns the version defined in `pyproject.toml`

### Fallback Mechanism
- Used when package metadata is not available
- Common during development with editable installs
- Must be manually kept in sync with `pyproject.toml`

## See Also

- [PEP 621 - Storing project metadata in pyproject.toml](https://peps.python.org/pep-0621/)
- [Python Packaging User Guide](https://packaging.python.org/)
