.PHONY: all build check clean dev-requirements

all: build

build: clean
	python3 setup.py sdist bdist_wheel

clean:
	rm -rf build dist

requirements:
	pip3 install -r requirements.txt
	pip3 install -r requirements-test.txt

lint:
	pylint --disable=R,C metriql-superset

type:
	mypy --ignore-missing-imports metriql-superset

check: build
	twine check dist/*

upload: check
	twine upload dist/*

dev-install: build
	pip3 uninstall -y metriql-superset && pip3 install dist/metriql_superset-*-py3-none-any.whl