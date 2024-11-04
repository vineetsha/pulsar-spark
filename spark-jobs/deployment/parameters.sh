#! /bin/sh -e
PACKAGE=fk-ekl-spark-jobs
PACKAGE_COMPASS=fk-ekl-spark-jobs
PACKAGE_BIGFOOT=fk-ekl-spark-bigfoot-ingestion
PACKAGE_BIGFOOT_V2=fk-ekl-spark-bigfoot-ingestion-v2
PACKAGE_GEOCODE=fk-ekl-spark-geocode-cache
PACKAGE_SMART_LOOKUP=fk-ekl-spark-smart-lookup
PACKAGE_GPS_PINGS=fk-ekl-spark-pings-ingestion
PACKAGE_DELIVERY_LOCATION_REFINEMENT=fk-ekl-delivery-location-refinement
PACKAGE_VIESTI_COMPASS_STREAMING=fk-ekl-viesti-compass-streaming
PACKAGE_USER=fk-pf-spark
PACKAGE_USERID=3069
PACKAGE_USERGROUP=fk-w3
PACKAGE_USERGROUPID=3000
LOG_DIR=/var/log/flipkart/supply-chain/${PACKAGE}/
LOG_FILENAME=${PACKAGE}.log
LOG_FILEPATH=${LOG_DIR}${LOG_FILENAME}

echo "Deployment env is $DEPLOYMENT_ENV"
if [ ${DEPLOYMENT_ENV} == "KLOUD" ]; then
   export BUCKET="fk-ekl-compass-spark-jobs-hyd"
elif [ ${DEPLOYMENT_ENV} == "STAGE" ]; then
   export BUCKET="$PACKAGE-stage"
elif [ ${DEPLOYMENT_ENV} == "PRODUCTION" ]; then
	export BUCKET="$PACKAGE-production"
else
   export BUCKET="$PACKAGE"
fi