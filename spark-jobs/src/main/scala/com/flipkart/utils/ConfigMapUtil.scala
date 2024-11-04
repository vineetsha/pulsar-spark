package com.flipkart.utils

import com.flipkart.config.Config

object ConfigMapUtil {
  final val configMap = Map[String, String](
    "bigfootDartEndpoint" -> Config.getProperty("Bigfoot.Dart.BatchIngestion.Url", "http://10.47.2.124:28223/ingestion/events/wsr/scp/ekl"),
    "geotagSchemaVersion" -> Config.getProperty("Bigfoot.GeoTag.SchemaVersion","5.0"),
    "eventSchemaVersion" -> Config.getProperty("Bigfoot.Event.SchemaVersion","1.0"),
    "bigfootGroupAmount" -> Config.getProperty("Bigfoot.Payload.Group.Amount", "200") ,
    "varadhiUrl" -> Config.getProperty("Varadhi.Ingestion.Url", "10.65.38.218"),
    "fsdExternalEndPoint" -> Config.getProperty("fsd.external.Url", "http://flo-fkl-app2.stage.ch.flipkart.com:27012/fsd-external-apis"),
    "facilityEndPoint" -> Config.getProperty("facility.Url", "http://flo-fkl-app5.stage.ch.flipkart.com:27747"),
    "userGeoCodingMsgQUrl" -> Config.getProperty("UserGeoCoding.MsgQ.url",""),
    "userGeoCodingServiceUrl" -> Config.getProperty("UserGeoCoding.Service.url",""),
    "mmiGeoCodingMsgQUrl" -> Config.getProperty("MmiGeoCoding.MsgQ.Url",""),
    "mmiGeoCodingServiceUrl" -> Config.getProperty("MmiGeoCoding.Service.Url",""),
    "cassandraHost" -> Config.getProperty("spark.StreamingApp.cassandraHost","").split(",").apply(0),
    "geoCoderEndPoint" -> Config.getProperty("geoCoder.Geocode.Url", "http://ekl-lmp-1.stage.nm.flipkart.com:5555")
  )

  final val zkPropertiesBigfoot = Map[String, String](
  "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.bigfootId", "bigfoot"),
  "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.ch.flipkart.com:2181")
  )

  final val zkPropertiesCompass = Map[String, String](
  "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.groupId", "compass"),
  "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "10.49.178.139:2181,10.50.18.132:2181,10.49.146.132:2181,10.51.66.163:2181,10.49.50.134:2181")
  )
}
