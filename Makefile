include makerules/python.mk

init::
	python -m pip install pip-tools
	python -m piptools sync task/dev-requirements.txt
	python -m pre_commit install
	python -m pip install -r requirements.txt
	

