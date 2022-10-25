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

RPM_DEPS?="python3-msgpack>=1.0.3,python3-pandas,python3-pytz"
RPM_NAME?="python3-tarantool"
rpm:
	python setup.py bdist_rpm --spec-only --requires="${RPM_DEPS}"
	mv dist/tarantool.spec ${HOME}/rpmbuild/SPECS/${RPM_NAME}.spec
	sed -i ${HOME}/rpmbuild/SPECS/${RPM_NAME}.spec -e 's/%define name tarantool/%define name ${RPM_NAME}/'
	python setup.py sdist
	set -e ;\
	VERSION=${find dist/tarantool-*.tar.gz | sed 's/^dist\/tarantool-\(.*\).tar.gz$/\1/'} ;\
	cp dist/tarantool-$${VERSION}.tar.gz ${HOME}/rpmbuild/SOURCES/tarantool-${RPM_NAME}.tar.gz ;\
	tar -xvzf dist/tarantool-${VERSION}.tar.gz ;\
	rm dist/tarantool-${VERSION}.tar.gz ;\
	mv -r tarantool-${VERSION} ${RPM_NAME}-${VERSION} ;\
	tar -cvzf ${HOME}/rpmbuild/SOURCES/${RPM_NAME}-${VERSION}.tar.gz ${RPM_NAME}-${VERSION} ;\
	rm -rf ${RPM_NAME}-${VERSION}
	rpmbuild -bb ~/rpmbuild/SPECS/python3-tarantool.spec
	
