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
	python setup.py bdist_rpm --spec-only --requires="python3-msgpack,python3-pandas,python3-pytz"
	sed -i dist/tarantool.spec -e 's/%define name tarantool/%define name python3-tarantool/'
	mv dist/tarantool.spec dist/python3-tarantool.spec