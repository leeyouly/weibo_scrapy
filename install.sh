#!/bin/bash
CURRENT_DIR=$(cd `dirname $0` && pwd -P)
cd $CURRENT_DIR
pip install -r requirements.txt || exit $?
VERSION=`python setup.py -V`
echo $VERSION
scrapyd-deploy -v $VERSION