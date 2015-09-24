#!/bin/bash

####  Usage ####
# ./run_python_test.sh [debug | release]

set -e
set -x

# Check build type
if [[ -z $1 ]]; then
        echo "build type must be specified. " 
        echo "Usage: $0 [debug | release ]"
        exit 1
fi
BUILD_TYPE=$1
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${SCRIPT_DIR}/..
oss_local_scripts/install_python_toolchain.sh
source oss_local_scripts/python_env.sh $BUILD_TYPE

# Make nosetest prints unity server log on exception
export DATO_VERBOSE=1

# run all python nosetests
cd $GRAPHLAB_BUILD_ROOT/oss_src/unity/python
make -j4

df -h

find . -name "*.xml" -delete
if ! type "parallel" > /dev/null; then
        cmd=""
        if [[ $OSTYPE != msys ]]; then
          cmd=${PYTHON_EXECUTABLE}
        fi
        cmd="${cmd} ${NOSETEST_EXECUTABLE} -v --with-id $PYTHONPATH/sframe/test --with-xunit --xunit-file=alltests.nosetests.xml"
        echo $cmd
        $cmd
else
        cmd=""
        if [[ $OSTYPE != msys ]]; then
          cmd=${PYTHON_EXECUTABLE}
        fi
        cmd="${cmd} ${NOSETEST_EXECUTABLE} -v --with-id -s --with-xunit --xunit-file={}.nosetests.xml --id-file={}.noseid {}"
        echo "Tests are running in parallel. Output is buffered until job is done..."
        find sframe/test -name "*.py" | parallel --group $cmd
fi

