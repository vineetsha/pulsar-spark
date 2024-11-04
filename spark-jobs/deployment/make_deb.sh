#! /bin/bash -e

set -e

echo	deployment/parameters.sh
. deployment/parameters.sh

PACKAGE_ROOT="./${PACKAGE}-package"
echo ${PACKAGE_ROOT}

## Setup Package Version and Debian Package Name
VERSION='0.0.'${BUILD_NUMBER}
if [ ${VERSION_PREFIX} ]; then
  VERSION=${VERSION_PREFIX}'.'${VERSION}
fi
ARCH=all
VERSION="${VERSION}"
DEB_NAME="${PACKAGE}_${VERSION}_${ARCH}"

## Install required compilers
#COMPILE_DEBS="oracle-java7-installer maven"
sudo apt-get install -y --force-yes ${COMPILE_DEBS}

#JAVA_HOME=/usr/lib/jvm/java-7-oracle
#export JAVA_HOME=${JAVA_HOME}

#if [ -z ${JAVA_HOME} ]; then
#  echo "JAVA_HOME env variable is not set. For sun jdk it could be something like /usr/lib/jvm/java-7-oracle"
#  exit 255
#fi

if dpkg -l ${COMPILE_DEBS}; then
  ## Cleaning
  echo "Removing the previous files..."
  rm -rf ${PACKAGE}*.deb
  rm -rf ${PACKAGE_ROOT}
  
  ## Creating Directory
  mkdir -p ${PACKAGE_ROOT}
  mkdir -p ${PACKAGE_ROOT}/var/lib/${PACKAGE}
  mkdir -p ${PACKAGE_ROOT}/etc/
  mkdir -p ${PACKAGE_ROOT}/etc/${PACKAGE}
  mkdir -p ${PACKAGE_ROOT}/DEBIAN
  mkdir -p ${PACKAGE_ROOT}/etc/init.d
#  mkdir -p $PACKAGE_ROOT/etc/$PACKAGE/META-INF
  mkdir -p ${PACKAGE_ROOT}/etc/logrotate.d/
  mkdir -p ${PACKAGE_ROOT}/etc/cosmos-jmx/
  mkdir -p ${PACKAGE_ROOT}/etc/rsyslog.d/
  mkdir -p ${PACKAGE_ROOT}/usr/local/ssl
  mkdir -p ${PACKAGE_ROOT}/var/log/flipkart/supply-chain/${PACKAGE}
  mkdir -p ${PACKAGE_ROOT}/var/lib/fk-pf-spark/conf/

  echo "PWD is"
  pwd
  ls
  
  ## Build Application
  #JAVA_HOME=${JAVA_HOME} mvn clean install -DskipTests
  
  echo "ls package_root"
  ls ${PACKAGE_ROOT}
  echo "ls current dir"
  ls
  cp target/spark-jobs-1.0-SNAPSHOT.jar		${PACKAGE_ROOT}/var/lib/${PACKAGE}/${PACKAGE}.jar

  ##Copy etc folder
  cp -r deployment/etc/* 		  ${PACKAGE_ROOT}/etc/

  ## Copying Debian Files
  cp deployment/deb/DEBIAN/* ${PACKAGE_ROOT}/DEBIAN/

  ##Change permissions
  chmod -R 775 ${PACKAGE_ROOT}/etc/${PACKAGE}/
  chmod -R 775 ${PACKAGE_ROOT}/DEBIAN/
  
  ## Create Service File
  cp deployment/service-spark-jobs ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_COMPASS}
  cp deployment/service-bigfoot-ingestion ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_BIGFOOT}
  cp deployment/service-geocode-cache ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_GEOCODE}
  cp deployment/service-smart-lookup ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_SMART_LOOKUP}
  cp deployment/service-gps-pings-ingestion ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_GPS_PINGS}
  cp deployment/service-delivery-location-refinement ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_DELIVERY_LOCATION_REFINEMENT}
  cp deployment/service-bigfoot-ingestion-v2 ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_BIGFOOT_V2}
  cp deployment/service-viesti-compass-streaming ${PACKAGE_ROOT}/etc/init.d/${PACKAGE_VIESTI_COMPASS_STREAMING}


  cp deployment/config/fkcloud_production/app.json							${PACKAGE_ROOT}/etc/cosmos-jmx/
  cp deployment/config/fkcloud_production/50-log-v2-${PACKAGE}.conf	        ${PACKAGE_ROOT}/etc/rsyslog.d/

  cp deployment/config/log4j-delivery-location-refinement.properties        ${PACKAGE_ROOT}/var/lib/fk-pf-spark/conf/
  cp deployment/config/log4j-gps-pings-ingestion.properties                 ${PACKAGE_ROOT}/var/lib/fk-pf-spark/conf/
  cp deployment/config/log4j-bigfoot-ingestion.properties                   ${PACKAGE_ROOT}/var/lib/fk-pf-spark/conf/
  cp deployment/config/log4j-compass-streaming.properties                   ${PACKAGE_ROOT}/var/lib/fk-pf-spark/conf/
  cp deployment/config/log4j-bigfoot-ingestion-v2.properties                ${PACKAGE_ROOT}/var/lib/fk-pf-spark/conf/
  cp deployment/config/log4j-viesti-compass-streaming.properties            ${PACKAGE_ROOT}/var/lib/fk-pf-spark/conf/

  ## Replace Placeholders
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/DEBIAN/control
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/DEBIAN/preinst
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/DEBIAN/postinst
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/DEBIAN/prerm
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/DEBIAN/postrm
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-spark-bigfoot-ingestion
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-spark-bigfoot-ingestion-v2
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-viesti-compass-streaming
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-spark-jobs
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-spark-geocode-cache
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-spark-smart-lookup
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-spark-pings-ingestion
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/init.d/fk-ekl-delivery-location-refinement
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/rsyslog.d/50-log-v2-${PACKAGE}.conf
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__PACKAGE_USER__/${PACKAGE_USER}/g" -e "s/__PACKAGE_USERID__/${PACKAGE_USERID}/g" -e "s/__PACKAGE_USERGROUP__/${PACKAGE_USERGROUP}/g" -e "s/__PACKAGE_USERGROUPID__/${PACKAGE_USERGROUPID}/g" -e "s|__LOG_DIR__|${LOG_DIR}|g" -e "s|__LOG_FILENAME__|${LOG_FILENAME}|g" -e "s|__LOG_FILEPATH__|${LOG_FILEPATH}|g" -e "s|__INGESTION_LOG_FILENAME__|${INGESTION_LOG_FILENAME}|g" -e "s|__INGESTION_LOG_FILEPATH__|${INGESTION_LOG_FILEPATH}|g" -e "s|__GC_LOG_FILENAME__|${GC_LOG_FILENAME}|g" -e "s|__GC_LOG_FILEPATH__|${GC_LOG_FILEPATH}|g" -e "s/__VERSION__/${VERSION}/g" -e "s/__ARCH__/${ARCH}/g" -e "s|__CONF_FILENAME__|${CONF_FILENAME}|g" -e "s|__CONF_FILEPATH__|${CONF_FILEPATH}|g" ${PACKAGE_ROOT}/etc/cosmos-jmx/app.json

#  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/config.yml.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-bigfoot-ingestion.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-bigfoot-ingestion-executor.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-compass-streaming.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-compass-streaming-executor.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-geocode-cache.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-geocode-cache-executor.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-smart-lookup.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-smart-lookup-executor.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-gps-pings-ingestion.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-gps-pings-ingestion-executor.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-delivery-location-refinement.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-delivery-location-refinement-executor.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-bigfoot-ingestion-v2.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-bigfoot-ingestion-v2-executor.toml"
  sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-viesti-compass-streaming.toml"
    sed -i${BACKUP_EXT} -e "s/__BUCKET__/${BUCKET}/g" -e "s/__EKL_SERVICE_CONFIG_BUCKET__/${EKL_SERVICE_CONFIG_BUCKET}/g" -e 's/__PACKAGE__/'"${PACKAGE}"'/g' "${PACKAGE_ROOT}/etc/confd/conf.d/log4j-viesti-compass-streaming-executor.toml"
  ## Create Debian Package
  dpkg-deb -b ${PACKAGE_ROOT}
  
  DEB_FILE="${DEB_NAME}.deb"
  echo "Moving the package to ${DEB_FILE}"
  mv ${PACKAGE_ROOT}.deb ${DEB_FILE}
  
  ## Push it to Repo Service
  REPO_VERSION_OUTPUT=`reposervice --host 10.24.0.41 --port 8080 pubrepo --repo ${PACKAGE} --appkey eklKey --debs ${DEB_FILE}`
  REPO_VERSION=`echo ${REPO_VERSION_OUTPUT}|cut -d'/' -f 6`
  echo ${REPO_VERSION_OUTPUT}
  echo "Repo version is"
  echo ${REPO_VERSION}
  
  ## Create Repo Env
  echo "Repo ENV to be created here"
  cp deployment/repo_env.tpl	${PACKAGE_ROOT}/
  sed -i -e "s/__PACKAGE__/${PACKAGE}/g" -e "s/__REPO_VERSION__/${REPO_VERSION}/g" ${PACKAGE_ROOT}/repo_env.tpl
  reposervice --host 10.24.0.41 --port 8080 putenv --name ${PACKAGE} --appkey eklKey --envdef ${PACKAGE_ROOT}/repo_env.tpl
  
  ## Cleaning
  rm -rf	${PACKAGE_ROOT}
  rm -rf	${PACKAGE}*.deb  
else
  echo "Please run apt-get install $COMPILE_DEPS"
  exit -1
fi