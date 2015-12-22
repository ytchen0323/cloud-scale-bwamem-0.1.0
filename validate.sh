#!/bin/bash

set -e

if [[ $# != 2 ]]; then
    echo usage: validate.sh ref-folder check-folder
    exit 1
fi

REF=$1
CHECK=$2
PWD=$(pwd)

FILES=$(cd $REF/ && find . -name "*.parquet" && cd $PWD)

for F in $FILES; do
    if [[ ! -f $CHECK/$F ]]; then
        echo Missing file $CHECK/$F
        exit 1
    fi
    set +e 
    diff $REF/$F $CHECK/$F > /dev/null
    ERR=$?
    set -e
    echo $F $ERR
done
