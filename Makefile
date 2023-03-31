include makerules/python.mk

init::
	python -m pip3 install pip-tools
	python -m pip3 install black
	python -m piptools sync task/requirements.txt
	