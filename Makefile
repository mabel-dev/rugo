lint:
	python -m uv pip install --quiet --upgrade pycln isort ruff yamllint cython-lint
#	python -m yamllint .
	cython-lint rugo/**/*.pyx
	python -m ruff check --fix --exit-zero
	python -m pycln .
	python -m isort .
	python -m ruff format rugo

update:
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade -r pyproject.toml

test:
	python -m uv pip install --upgrade pytest pytest-xdist
	clear
	export MANUAL_TEST=1
	python -m pytest -n auto --color=yes

mypy:
	clear
	python -m pip install --upgrade mypy
	python -m mypy --ignore-missing-imports --python-version 3.11 --no-strict-optional --check-untyped-defs rugo

coverage:
	clear
	export MANUAL_TEST=1
	python -m coverage run -m pytest --color=yes
	python -m coverage report --include=rugo/** --fail-under=80 -m

compile:
	clear
	python -m pip install --upgrade pip uv
	python -m uv pip install --upgrade cython setuptools
	find . -name '*.so' -delete
	rm -rf build dist *.egg-info
	python setup.py clean
	python setup.py build_ext --inplace -j 8

verify-version:
	python verify_version.py
