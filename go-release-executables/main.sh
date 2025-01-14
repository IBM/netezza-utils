#!/bin/bash

set -x

#!/bin/bash -eux

GO_LINUX_PACKAGE_URL="https://dl.google.com/go/go1.14.linux-amd64.tar.gz"
wget --no-check-certificate --progress=dot:mega ${GO_LINUX_PACKAGE_URL} -O go-linux.tar.gz
tar -zxf go-linux.tar.gz
sudo mv go /usr/local/
sudo mkdir -p /go/bin /go/src /go/pkg

export GO_HOME=/usr/local/go
export GOPATH=/go
export PATH=${GOPATH}/bin:${GO_HOME}/bin/:$PATH

THIS_GITHUB_EVENT=$(cat $GITHUB_EVENT_PATH)
RELEASE_UPLOAD_URL=$(echo $THIS_GITHUB_EVENT | jq -r .release.upload_url)
RELEASE_UPLOAD_URL=${RELEASE_UPLOAD_URL/\{?name,label\}/}
RELEASE_TAG_NAME=$(echo $THIS_GITHUB_EVENT | jq -r .release.tag_name)
PROJECT_NAME=$(basename $GITHUB_REPOSITORY)

EXECUTABLE_FILES=`sudo bash ./build.sh $GITHUB_REPOSITORY $GITHUB_WORKSPACE $GOOS $EXECUTABLE_NAME $SUBDIR`
EXECUTABLE_FILES=`echo "${EXECUTABLE_FILES}" | awk '{$1=$1};1'`

PROJECT_ROOT="/go/src/github.com/${GITHUB_REPOSITORY}"
TMP_ARCHIVE=tmp.tgz
CKSUM_FILE=md5sum.txt

md5sum ${PROJECT_ROOT}/${SUBDIR}/${EXECUTABLE_FILES} | cut -d ' ' -f 1 > ${CKSUM_FILE}
tar cvfz ${TMP_ARCHIVE} ${CKSUM_FILE} --directory ${PROJECT_ROOT}/${SUBDIR} ${EXECUTABLE_FILES}

NAME="${NAME:-${EXECUTABLE_FILES}_${RELEASE_TAG_NAME}}_${GOOS}_${GOARCH}"

curl \
  --tlsv1.2 \
  -X POST \
  --data-binary @${TMP_ARCHIVE} \
  -H 'Content-Type: application/octet-stream' \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  "${RELEASE_UPLOAD_URL}?name=${NAME}.${TMP_ARCHIVE/tmp./}"
