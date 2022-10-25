#!/bin/sh

set -euo pipefail

rpm_name='python3-tarantool'

echo "Build spec..."

rpm_deps='python3-msgpack,python3-pandas,python3-pytz'

SETUPTOOLS_SCM_DEBUG=1 python3 setup.py bdist_rpm --spec-only --requires="$rpm_deps"
mv dist/tarantool.spec $HOME/rpmbuild/SPECS/$rpm_name.spec
sed -i $HOME/rpmbuild/SPECS/$rpm_name.spec -e 's/%define name tarantool/%define name '$rpm_name'/'

echo "Build sources..."

python3 setup.py sdist

regex='s/^dist\/tarantool-\(.*\).tar.gz$/\1/'
version=$(find dist/tarantool-*.tar.gz | sed $regex)

tar -xvzf dist/tarantool-$version.tar.gz
rm dist/tarantool-$version.tar.gz
mv tarantool-$version $rpm_name-$version
tar -cvzf $HOME/rpmbuild/SOURCES/$rpm_name-$version.tar.gz $rpm_name-$version
rm -rf $rpm_name-$version

echo "Build rpm..."

rpmbuild -bb ~/rpmbuild/SPECS/python3-tarantool.spec
