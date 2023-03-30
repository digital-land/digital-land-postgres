init::
	python -m pip install pip-tools
	python -m piptools sync task/requirements.txt
	python -m pre_commit install
	npm install

lint:	black-check flake8

black:
	black .

black-check:
	black --check .

flake8:
	flake8 .