include makerules/python.mk

init::
	python -m pip install pip-tools
	python -m pip install black
	python -m pip install mypy_extensions
	python -m pip install pathspec
	python -m pip install platformdirs
	python -m pip install typing-extensions
	python -m piptools sync task/dev-requirements.txt
	