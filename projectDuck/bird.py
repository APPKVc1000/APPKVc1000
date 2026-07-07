import os
import runpy
import shutil
import sys


def cache_deletion(path=os.getcwd()):
    for root, dirs, files in os.walk(path):
        for dir in dirs:
            if dir == "__pycache__":
                shutil.rmtree(os.path.join(root, dir))


if __name__ == "__main__":
    cache_deletion()
    runpy.run_module("projectDuck.spiders.duck", run_name="__main__")
    sys.exit()
