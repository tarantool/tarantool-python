#!/bin/sh

set -exu # Strict shell (w/o -o pipefail)

# Install tarantool.
curl http://download.tarantool.org/tarantool/2x/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`
echo "deb http://download.tarantool.org/tarantool/2x/ubuntu/ ${release} main" | sudo tee /etc/apt/sources.list.d/tarantool_2x.list
sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool

# Install testing dependencies.
pip install -r requirements.txt
pip install pyyaml

# Run tests.
python setup.py test
