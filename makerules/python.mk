PIP_INSTALL_PACKAGE=[test]

all:: lint test coverage

lint:: black-check

black-check:
	black --check .

black:
	black .

flake8:
	flake8 .

test-unit:
	python -m pytest

coverage:
	coverage run --source $(PACKAGE) -m pytest && coverage report

coveralls:
	py.test --cov $(PACKAGE) tests/ --cov-report=term --cov-report=html

bump::
	git tag $(shell python version.py)

dist:: all
	python setup.py sdist bdist_wheel

upload::	dist
	twine upload dist/*

makerules::
	curl -qfsL '$(SOURCE_URL)/makerules/main/python.mk' > makerules/python.mk
