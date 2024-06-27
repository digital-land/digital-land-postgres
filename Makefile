init::
	python -m pip install --upgrade pip
	python -m pip install pip-tools
	python -m piptools compile task/requirements/dev-requirements.in
	python -m piptools compile task/requirements/requirements.in
	python -m piptools sync task/requirements/dev-requirements.txt task/requirements/requirements.txt
	python -m pre_commit install

reqs::
	python -m piptools compile task/requirements/dev-requirements.in
	python -m piptools compile task/requirements/requirements.in
	python -m piptools sync task/requirements/requirements.txt task/requirements/dev-requirements.txt


upgrade::
	python -m piptools compile --upgrade task/requirements/dev-requirements.in
	python -m piptools compile --upgrade task/requirements/requirements.in
	python -m piptools sync task/requirements/requirements.txt task/requirements/dev-requirements.txt

test:: test-integration

test-integration:
	python -m pytest tests/integration

# TODO add flake8 back in to the linting
lint::
	black .
