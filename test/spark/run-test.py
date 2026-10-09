import os
import sys


if __name__ == "__main__":
    script = os.path.join(os.path.dirname(__file__), "run-test.sh")
    os.execvp("bash", ["bash", script, *sys.argv[1:]])
