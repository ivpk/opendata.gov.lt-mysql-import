env/bin/py.test: env/bin/pip requirements-dev.txt env/src/ckan/requirements.txt
	env/bin/pip install -r requirements-dev.txt

env/bin/pip-tools: env/bin/pip
	env/bin/pip install pip-tools
	touch -c env/bin/pip-tools

env/bin/pip:
	virtualenv -p /usr/bin/python2 env

requirements-dev.txt: env/bin/pip-tools requirements-dev.in
	env/bin/pip-compile requirements-dev.in

env/src/ckan/requirements.txt: env/bin/pip
	env/bin/pip install -e 'git+https://github.com/ckan/ckan.git#egg=ckan'
	env/bin/pip install -r env/src/ckan/requirements.txt -r env/src/ckan/dev-requirements.txt
	touch -c env/src/ckan/requirements.txt

.PHONY: test
test: env/bin/py.test
	env/bin/py.test tests
