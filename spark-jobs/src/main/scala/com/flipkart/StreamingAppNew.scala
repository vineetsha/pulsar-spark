package com.flipkart

import com.flipkart.config.Config
import com.flipkart.core.CryptexInitializer
import com.flipkart.messaging.KafkaPipeline
import com.flipkart.service._
import com.flipkart.utils.ConfigMapUtil
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingAppNew {

  def main(args: Array[String]){

    lazy val sparkMaxCores = Config.getProperty("spark.StreamingApp.max.cores", "10")
    lazy val sparkExecutorMemory = Config.getProperty("spark.StreamingApp.executor.memory", "5g")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.52.18.196:7077,10.51.114.181:7077")
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.52.178.198,10.52.178.198,10.50.226.208,10.51.130.240,10.52.210.209,10.50.178.228,10.50.34.232,10.52.130.185")
    lazy val datacenter = Config.getProperty("spark.cassandra.connection.local_dc", "datacenter1")
    lazy val username = Config.getProperty("spark.cassandra.auth.username", "cassandra")
    lazy val cryptexInitializer = new CryptexInitializer()
    lazy val password = if (cryptexInitializer.isCryptexEnabled) cryptexInitializer.decrypt(
      Config.getProperty("spark.cassandra.auth.password", "cassandra"))
    else Config.getProperty("spark.cassandra.auth.password", "cassandra")
    lazy val appName = Config.getProperty("spark.StreamingApp.appName", "Compass Streaming App")
    lazy val kafkaBrokers = Config.getProperty("spark.StreamingApp.kafkaBrokers", "10.49.66.142:9092,10.50.2.199:9092,10.50.210.208:9092")
    lazy val numPartitions = Config.getProperty("spark.StreamingApp.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.StreamingApp.repartition","false")
    lazy val kafkaMaxRatePerPartition = Config.getProperty("spark.streaming.kafka.maxRatePerPartition","10")
//    lazy val geocodeCachePincodeList= Config.getProperty("geocode.geocodeCachePincodeList","560004,560018")


    var clusterScoreConfig = Map[String, String]()
    clusterScoreConfig += "distanceThreshold" -> Config.getProperty("cluster-score-configuration.distanceThreshold", "200")
    clusterScoreConfig += "minPointsCount" -> Config.getProperty("cluster-score-configuration.minPointsCount", "3")
    clusterScoreConfig += "majorityPercentage" -> Config.getProperty("cluster-score-configuration.majorityPercentage", "50")
    clusterScoreConfig += "driftThreshold" -> Config.getProperty("cluster-score-configuration.driftThreshold", "200")

    var configMap = Map[String, String]()
    configMap += "userGeoCodingMsgQUrl" -> Config.getProperty("UserGeoCoding.Url", "")
    configMap += "mmiGeoCodingMsgQUrl" -> Config.getProperty("MmiGeoCoding.Url","")
    configMap += "fsdExternalEndPoint" -> Config.getProperty("fsd.external.Url","http://10.65.30.80/fsd-external-apis")
    configMap += "geoCoderEndPoint" -> Config.getProperty("geoCoder.Geocode.Url","http://10.84.31.227:8888")
    configMap += "facilityEndPoint" -> Config.getProperty("facility.Url","http://10.84.30.132")
    configMap += "geocodeCachePincodeList" -> Config.getProperty("geocode.geocodeCachePincodeList","560004,560018")

    var zkPropertiesTripTracker = Map[String, String]()
    zkPropertiesTripTracker += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.trip_tracker", "trip_tracker")
    zkPropertiesTripTracker += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "10.49.178.139:2181,10.50.18.132:2181,10.49.146.132:2181,10.51.66.163:2181,10.49.50.134:2181")

//    var zkPropertiesShipmentGeocodeFeed = Map[String, String]()
//    zkPropertiesShipmentGeocodeFeed += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.shipment_geocode_feed", "shipment_geocode_feed")
//    zkPropertiesShipmentGeocodeFeed += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.nm.flipkart.com:2181")

    lazy val streamingInterval = Config.getProperty("Compass.App.Interval","10").toLong
    val sparkConfig = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.local_dc", datacenter)
      .set("spark.cassandra.auth.username", username)
      .set("spark.cassandra.auth.password", password)
      .set("spark.cassandra.connection.keep_alive_ms",(streamingInterval*1000 + 100).toString)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming-executor.properties -Dcom.sun.management.jmxremote.port=25397 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", kafkaMaxRatePerPartition)

    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sparkConfig, Seconds(streamingInterval))
    try {

      val kafkaStringStreamGeotag = new KafkaPipeline(ssc, "geotag_feed", kafkaBrokers, ConfigMapUtil.zkPropertiesCompass).getStream
      //val kafkaStringStreamEvent = new KafkaPipeline(ssc, "event_feed", kafkaBrokers, zkProperties).getStream
//      val kafkaStringStreamSignature = new KafkaPipeline(ssc, "signature_feed", kafkaBrokers, zkProperties).getStream
      // val kafkaStringStreamTripTracker = new KafkaPipeline(ssc, "event_feed", kafkaBrokers, zkPropertiesTripTracker).getStream
      //val kafkaStringStreamShipmentGeocodeFeed = new KafkaPipeline(ssc,"shipment_feed_bigfoot", kafkaBrokers, zkPropertiesShipmentGeocodeFeed).getStream

      Logger.log(this.getClass, INFO, BaseSLog(s"Starting Data Ingestion"))
      KafkaStreamingService.processAndSaveToCassandra(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamGeotag, clusterScoreConfig, ssc)

      //GeoTagService.saveDataToCassandra(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamGeotag, zkProperties)
     // EventService.saveDataToCassandra(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamEvent, zkProperties)
//      SignatureService.saveDataToCassandra(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamSignature, zkProperties)
//      TripTrackingService.applyFunctionForEachRDD(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamTripTracker, zkPropertiesTripTracker, configMap, TripTrackingService.processEventForTripAlert)
      //GeoCodeService.applyFunctionForEachRDD(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamShipmentGeocodeFeed, zkPropertiesShipmentGeocodeFeed, configMap, GeoCodeService.buildGeocodeCache)
      // if you want to create single DirectStream this code will come in handy
      /*kafkaStringStream.foreachRDD{rdd =>
        val topics = rdd.map(_._1).distinct().collect()
        if (topics.length > 0) {
          val rdd_value = rdd.take(10).mkString("\n.....\n")
          Logger.log(this.getClass, INFO, BaseSLog(s"Printing all feeds\n$rdd_value"))

          topics.foreach { topic =>
            val filteredRdd = rdd.collect { case (t, data) if t == topic => data }
            CassandraFactory.getCassandraModel(topic).saveDataToCassandra(topic,filteredRdd)
          }
          //once everything above is done, then only update the offsets
          ZookeeperManager.updateOffsetsinZk(rdd)
        }
      }
      */
      ssc.start()
      ssc.awaitTermination()
    }
    catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing StreamingApp: " + e.getMessage, e))
        throw e
    }
    finally {
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping StreamingApp"))
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        Logger.log(this.getClass, ERROR, BaseSLog(s"StreamingApp stopped"))
      }
    }
  }
}
