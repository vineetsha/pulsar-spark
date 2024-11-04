#! /bin/sh -e

set -e

echo	${WORKSPACE}/magnet/deployment/parameters.sh
.	${WORKSPACE}/magnet/deployment/parameters.sh

TMP_DIR=tmp

rm -rf ${TMP_DIR}
## Creating Directory
mkdir -p ${TMP_DIR}

cp deployment/node_setup.sh	tmp/
sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_REPO_ENV__/${PACKAGE}/g" -e "s/__PACKAGE_REPO_ENV_VERSION__/${PACKAGE_REPO_ENV_VERSION}/g" ${TMP_DIR}/node_setup.sh

echo =========
echo "kloud-cli --endpoint=beta instance --appId=${PACKAGE} create --type=c1.medium --script=$TMP_DIR/node_setup.sh"
echo =========
cat ${TMP_DIR}/node_setup.sh
echo
echo =========