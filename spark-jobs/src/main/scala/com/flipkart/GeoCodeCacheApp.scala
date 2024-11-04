package com.flipkart

import com.datastax.spark.connector.cql.CassandraConnector
import com.flipkart.config.Config
import com.flipkart.messaging.KafkaPipeline
import com.flipkart.service.{GeoCodeService}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by shivang.b on 19/08/16.
 */
object GeoCodeCacheApp {

  def main(args: Array[String]) {

    lazy val sparkMaxCores = Config.getProperty("spark.StreamingApp.max.cores", "10")
    lazy val sparkExecutorMemory = Config.getProperty("spark.StreamingApp.executor.memory", "5g")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.75.123.103:7077,10.75.123.104:7077,10.75.123.105:7077")
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.75.123.103")
    lazy val appName = Config.getProperty("spark.GeoCodeCache.appName", "GeoCode Cache App")
    lazy val kafkaBrokers = Config.getProperty("spark.StreamingApp.kafkaBrokers", "ekl-lmp-1.stage.ch.flipkart.com:9092")
    lazy val numPartitions = Config.getProperty("spark.GeoCodeApp.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.GeoCodeApp.repartition", "false")
    lazy val kafkaMaxRatePerPartition = Config.getProperty("spark.streaming.kafka.maxRatePerPartition", "10")
    lazy val geocodeCachePincodeList = Config.getProperty("geocode.geocodeCachePincodeList", "560004,560018")

    var configMap = Map[String, String]()
    configMap += "userGeoCodingMsgQUrl" -> Config.getProperty("UserGeoCoding.Url", "")
    configMap += "mmiGeoCodingMsgQUrl" -> Config.getProperty("MmiGeoCoding.Url", "")
    configMap += "fsdExternalEndPoint" -> Config.getProperty("fsd.external.Url", "http://10.65.30.80/fsd-external-apis")
    configMap += "geoCoderEndPoint" -> Config.getProperty("geoCoder.Geocode.Url", "http://10.84.31.227:8888")
    configMap += "facilityEndPoint" -> Config.getProperty("facility.Url", "http://10.84.30.132")
    configMap += "geocodeCachePincodeList" -> Config.getProperty("geocode.geocodeCachePincodeList", "560004,560018")
    configMap += "pincodeInfoEndPoint" -> Config.getProperty("geoCoder.pincodeInfo.Url", "http://10.85.51.175")
    configMap += "spyglassEndPoint" -> Config.getProperty("spyglass.Url", "http://10.65.38.178")


    var zkPropertiesShipmentGeocodeFeed = Map[String, String]()
    zkPropertiesShipmentGeocodeFeed += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.shipment_geocode_feed", "shipment_geocode_feed")
    zkPropertiesShipmentGeocodeFeed += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.nm.flipkart.com:2181")

    lazy val streamingInterval = Config.getProperty("GeoCode.App.Interval", "120").toLong
    val sc = new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.keep_alive_ms", (streamingInterval * 1000 + 100).toString)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-geocode-cache-executor.properties -Dcom.sun.management.jmxremote.port=25597 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", kafkaMaxRatePerPartition)

    val cassandraConnector = CassandraConnector(sc)
    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sc, Seconds(streamingInterval))
    try {
      val kafkaStringStreamShipmentGeocodeFeed = new KafkaPipeline(ssc, "shipment_feed_bigfoot", kafkaBrokers, zkPropertiesShipmentGeocodeFeed).getStream

      GeoCodeService.applyFunctionForEachRDD(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamShipmentGeocodeFeed, zkPropertiesShipmentGeocodeFeed, configMap, GeoCodeService.buildGeocodeCache)

      ssc.start()
      ssc.awaitTermination()
    }
    catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing GeoCodeCacheApp: " + e.getMessage, e))
        throw e
    }
    finally {
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping StreamingApp"))
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        Logger.log(this.getClass, ERROR, BaseSLog(s"GeoCodeCacheApp stopped"))
      }
    }
  }

}
