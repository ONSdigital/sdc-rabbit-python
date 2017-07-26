
.PHONY: depinstall clean dist test

all: install test

clean:
	rm -rf sdc/rabbit/doc/html
	rm -v dist/sdc-rabbit-*.tar.gz

install:
	pip3 install -r requirements.txt

dist:
	python setup.py sdist

test: depinstall
	pip3 install -r test_requirements.txt
	flake8 .
	coverage run --branch --source=sdc.rabbit -m unittest sdc/rabbit/test/*.py
