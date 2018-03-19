env/.done: env/bin/pip setup.py requirements-dev.txt
	env/bin/pip install -e .
	env/bin/pip install -r requirements-dev.txt
	env/bin/pip install -r env/src/ckan/requirements.txt
	env/bin/pip install -r env/src/ckan/dev-requirements.txt
	env/bin/pip install -r env/src/ckanext-harvest/pip-requirements.txt
	env/bin/pip install -r env/src/ckanext-harvest/dev-requirements.txt
	touch env/.done

env/bin/pip-tools: env/bin/pip
	env/bin/pip install pip-tools
	touch -c env/bin/pip-tools

env/bin/pip:
	virtualenv -p /usr/bin/python2 env

env/src/ckan/requirements.txt: env/bin/pip
	touch -c env/src/ckan/requirements.txt

requirements-dev.txt: requirements-dev.in
	env/bin/pip-compile requirements-dev.in

.PHONY: test
test: env/bin/py.test
	env/bin/py.test -vvx --tb=native --cov-report=term-missing --cov=odgovlt tests

.PHONY: tags
tags:
	ctags --recurse odgovlt.py env/src env/lib
