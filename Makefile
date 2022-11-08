.PHONY: install
install:
	pip3 install --editable .


.PHONY: test
test:
	python3 setup.py test

.PHONY: test-pure-install
test-pure-install:
	TEST_PURE_INSTALL=true python3 -m unittest discover -v

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


.PHONY: pip-sdist
pip-sdist:
	python3 setup.py sdist --dist-dir=pip_dist

.PHONY: pip-bdist
pip-bdist:
	python3 setup.py bdist_wheel --dist-dir=pip_dist

.PHONY: pip-dist
pip-dist: pip-sdist pip-bdist

.PHONY: pip-dist-check
pip-dist-check:
	twine check pip_dist/*


.PHONY: rpm-dist
rpm-dist:
	python3 setup.py sdist --dist-dir=rpm/SOURCES
	rpmbuild -ba --define "_topdir `pwd`/rpm" rpm/SPECS/python-tarantool.spec
	mkdir -p rpm_dist
	mv rpm/SRPMS/*.rpm -t rpm_dist
	mv rpm/RPMS/noarch/*.rpm -t rpm_dist

.PHONY: rpm-dist-check
rpm-dist-check:
	rpm -K --nosignature rpm_dist/*.rpm


.PHONY: deb-changelog-entry
deb-changelog-entry:
	DEBEMAIL=admin@tarantool.org dch --distribution unstable -b \
									 --package "python3-tarantool" \
									 --newversion $$(python3 setup.py --version) \
									 "Nightly build"

.PHONY: deb-dist
deb-dist:
	dpkg-source -b .
	dpkg-buildpackage -rfakeroot -us -uc
	mkdir -p deb_dist
	find .. -maxdepth 1 -type f -regex '.*/python3-tarantool_.*\.deb' \
							-or -regex '.*/python3-tarantool_.*\.buildinfo' \
							-or -regex '.*/python3-tarantool_.*\.changes' \
							-or -regex '.*/python3-tarantool_.*\.dsc' \
							-or -regex '.*/python3-tarantool_.*\.tar\.xz' \
							| xargs -I {} mv {} deb_dist/

.PHONY: deb-dist-check
deb-dist-check:
	dpkg -I deb_dist/*.deb
