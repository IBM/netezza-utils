#!/bin/bash

set -x

PROJECT_ROOT="/go/src/github.com/${GITHUB_REPOSITORY}"
PROJECT_NAME=$(basename $GITHUB_REPOSITORY)

mkdir -p $PROJECT_ROOT
rmdir $PROJECT_ROOT
ln -s $GITHUB_WORKSPACE $PROJECT_ROOT
cd $PROJECT_ROOT
go get -v ./...

if [ -x "./build.sh" ]; then
  OUTPUT=`./build.sh ${BUILD_OPTS}`
else
  go build ${BUILD_OPTS}
  COMPILED_FILES="${PROJECT_NAME}"
fi

echo ${COMPILED_FILES}