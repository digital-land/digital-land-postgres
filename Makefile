include makerules/python.mk

init::
	python -m pip install pip-tools
	python -m pip install black
	python -m piptools sync task/requirements.txt
	