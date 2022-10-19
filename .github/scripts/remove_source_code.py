import os, shutil

required_paths = ['.git', '.github', 'test', 'requirements-test.txt', 'Makefile']

for path in os.listdir():
    if path in required_paths:
        continue

    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    else:
        raise ValueError(f"{path} is not a file, link or dir")
