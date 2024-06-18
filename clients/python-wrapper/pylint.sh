#!/bin/sh

#pip install pylint --user -qq
#pip install -r requirements.txt -qq
python -m pylint tests --rc=tests/.pylintrc 
python -m pylint lakefs --rc=lakefs/.pylintrc