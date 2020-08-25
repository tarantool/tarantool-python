#!/bin/sh

set -exu # Strict shell (w/o -o pipefail)

# Install tarantool.
curl http://download.tarantool.org/tarantool/2x/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`
echo "deb http://download.tarantool.org/tarantool/2x/ubuntu/ ${release} main" | sudo tee /etc/apt/sources.list.d/tarantool_2x.list
sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool

# Install module requirements.
#
# Keep it in sync with requirements.txt.
pip install "${PYTHON_MSGPACK:-msgpack==1.0.0}"
python -c 'import msgpack; print(msgpack.version)'

# Install testing dependencies.
pip install pyyaml dbapi-compliance==1.15.0

# Run tests.
python setup.py test
