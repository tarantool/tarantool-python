# pylint: disable=missing-module-docstring
import os

import tarantool

socket_fd = int(os.environ["SOCKET_FD"])

conn = tarantool.connect(None, None, socket_fd=socket_fd)

# Check user.
assert conn.eval("return box.session.user()").data[0] == "test"

# Check db operations.
conn.insert("test", [1])
conn.insert("test", [2])
assert conn.select("test").data == [[1], [2]]
