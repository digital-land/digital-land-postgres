init::
	python -m pip install pip-tools
	python -m piptools sync task/dev-requirements.txt
	python -m pre_commit install
	python -m pip install -r task/requirements.txt

test:: test-integration

test-integration:
	python -m pytest --cov task tests/integration

# TODO add flake8 back in to the linting
lint::
	black .
