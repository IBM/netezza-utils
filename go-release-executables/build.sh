#!/bin/bash

set -x

GITHUB_REPOSITORY=$1
GITHUB_WORKSPACE=$2
GOOS=$3
EXECUTABLE_NAME=$4
SUBDIR=$5
GOARCH=$6

export GO_HOME=/usr/local/go
export GOPATH=/go
export PATH=${GOPATH}/bin:${GO_HOME}/bin/:$PATH

PROJECT_ROOT="/go/src/github.com/${GITHUB_REPOSITORY}"
PROJECT_NAME=$(basename $GITHUB_REPOSITORY)

sudo mkdir -p $PROJECT_ROOT
sudo rmdir $PROJECT_ROOT
# without above, following symlink creation fails (?)
sudo ln -s $GITHUB_WORKSPACE $PROJECT_ROOT
cd $PROJECT_ROOT/${SUBDIR}
go get -v ./...

EXT=''

if [ $GOOS == 'windows' ]; then
EXT='.exe'
fi

if [ -z "${EXECUTABLE_NAME}" ]; then
  OUTFILE=${PROJECT_NAME}${EXT}
else
  OUTFILE=${EXECUTABLE_NAME}${EXT}
fi

if [ -x "./build.sh" ]; then
  COMPILED_FILES=`./build.sh "${OUTFILE}" "${BUILD_OPTS}"`
else
  GOOS=$GOOS GOARCH=$GOARCH sgo build -o ${OUTFILE} "${BUILD_OPTS}"
  COMPILED_FILES="${OUTFILE}"
fi

echo ${COMPILED_FILES}
