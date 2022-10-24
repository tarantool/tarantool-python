.PHONY: install test docs rpm
install:
	pip install --editable .
test:
	python setup.py test
testdata:
	cd ./test/data/; ./generate.sh
coverage:
	python -m coverage run -p --source=. setup.py test
cov-html:
	python -m coverage html -i
cov-report:
	python -m coverage report
dist:
	python setup.py sdist --format=gztar,bztar,zip
dist-upload:
	python setup.py sdist --format=gztar,bztar,zip upload
dist-upload-2:
	python setup.py sdist --format=ztar upload
docs:
	python setup.py build_sphinx
rpm-spec:
rpm:
	cp setup.py setup_rpm.py
	sed -i setup_rpm.py -e 's/name="tarantool"/name="tarantool-python"/'
	python setup_rpm.py bdist_rpm --requires="python3-msgpack,python3-pandas,python3-pytz" 
