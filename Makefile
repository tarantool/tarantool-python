.PHONY: install test docs
install:
	pip3 install --editable .
test:
	python3 setup.py test
testdata:
	cd ./test/data/; ./generate.sh
coverage:
	python3 -m coverage run -p --source=. setup.py test
cov-html:
	python3 -m coverage html -i
cov-report:
	python3 -m coverage report
docs:
	python3 setup.py build_sphinx
