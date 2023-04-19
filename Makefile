include makerules/python.mk

init::
	python -m pip install pip-tools
	python -m piptools sync task/dev-requirements.txt
	python -m pre_commit install
	
test-unit:
	python -m pytest --md-report --md-report-color=never --md-report-output=unit-tests.md tests/unit

test-integration:
	python -m pytest --md-report --md-report-color=never --md-report-output=integration-tests.md tests/integration

test-acceptance:
	python -m playwright install chromium
	python -m pytest --md-report --md-report-color=never -p no:warnings tests/acceptance

