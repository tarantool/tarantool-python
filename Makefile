.PHONY: install
install:
	pip3 install --editable .


.PHONY: test
test:
	python3 setup.py test

.PHONY: testdata
testdata:
	cd ./test/data/; ./generate.sh

.PHONY: coverage
coverage:
	python3 -m coverage run -p --source=. setup.py test

.PHONY: cov-html
cov-html:
	python3 -m coverage html -i

.PHONY: cov-report
cov-report:
	python3 -m coverage report


.PHONY: docs
docs:
	python3 setup.py build_sphinx
