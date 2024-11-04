package com.flipkart

import java.time.{LocalDateTime, ZoneOffset}

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.streaming._
import com.flipkart.config.Config
import com.flipkart.core.CryptexInitializer
import com.flipkart.service.DeliveryLocationRefinementService
import com.flipkart.utils.DateUtils
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level.{ERROR, INFO}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Minutes, StreamingContext}

object DeliveryLocationRefinementApp {

  def main(args: Array[String]){
    lazy val sparkMaxCores = Config.getProperty("spark.DeliveryLocationRefinementApp.max.cores", "15")
    lazy val sparkExecutorMemory = Config.getProperty("spark.DeliveryLocationRefinementApp.executor.memory", "10g")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.52.18.196:7077,10.51.114.181:7077")
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.52.178.198,10.52.178.198,10.50.226.208,10.51.130.240,10.52.210.209,10.50.178.228,10.50.34.232,10.52.130.185")
    lazy val datacenter = Config.getProperty("spark.cassandra.connection.local_dc", "datacenter1")
    lazy val username = Config.getProperty("spark.cassandra.auth.username", "cassandra")
    lazy val cryptexInitializer = new CryptexInitializer()
    lazy val password = if (cryptexInitializer.isCryptexEnabled) cryptexInitializer.decrypt(
      Config.getProperty("spark.cassandra.auth.password", "cassandra"))
    else Config.getProperty("spark.cassandra.auth.password", "cassandra")
    lazy val appName = Config.getProperty("spark.DeliveryLocationRefinementApp.appName", "Delivery Location Refinement App")
    lazy val numPartitions = Config.getProperty("spark.StreamingApp.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.StreamingApp.repartition","false")

    lazy val streamingInterval = Config.getProperty("DeliveryLocationRefinement.App.Interval","120").toLong

    var clusterScoreConfig = Map[String, String]()
    clusterScoreConfig += "distanceThreshold" -> Config.getProperty("cluster-score-configuration.distanceThreshold", "200")
    clusterScoreConfig += "minPointsCount" -> Config.getProperty("cluster-score-configuration.minPointsCount", "3")
    clusterScoreConfig += "majorityPercentage" -> Config.getProperty("cluster-score-configuration.majorityPercentage", "50")
    clusterScoreConfig += "driftThreshold" -> Config.getProperty("cluster-score-configuration.driftThreshold", "200")

    var pingsTriangulationConfig = Map[String, String]()
    pingsTriangulationConfig += "clusterScoreDistanceThreshold" -> Config.getProperty("pings-triangulation-configuration.clusterScoreDistanceThreshold", "200")
    pingsTriangulationConfig += "clusterScoreMinPointsCount" -> Config.getProperty("pings-triangulation-configuration.clusterScoreMinPointsCount", "3")
    pingsTriangulationConfig += "clusterScoreMajorityPercentage" -> Config.getProperty("pings-triangulation-configuration.clusterScoreMajorityPercentage", "50")
    pingsTriangulationConfig += "clusterScoreDriftThreshold" -> Config.getProperty("pings-triangulation-configuration.clusterScoreDriftThreshold", "200")
    pingsTriangulationConfig += "dbScanEps" -> Config.getProperty("pings-triangulation-configuration.dbScanEps", "50")
    pingsTriangulationConfig += "dbScanMinPoints" -> Config.getProperty("pings-triangulation-configuration.dbScanMinPoints", "3")
    pingsTriangulationConfig += "pingsWindowStartInMillis" -> Config.getProperty("pings-triangulation-configuration.pingsWindowStartInMillis", "-120000")
    pingsTriangulationConfig += "pingsWindowEndInMillis" -> Config.getProperty("pings-triangulation-configuration.pingsWindowEndInMillis", "300000")
    pingsTriangulationConfig += "pingsAccuracyThreshold" -> Config.getProperty("pings-triangulation-configuration.pingsAccuracyThreshold", "100")
    pingsTriangulationConfig += "deliveredAccuracyThreshold" -> Config.getProperty("pings-triangulation-configuration.deliveredAccuracyThreshold", "100")
    pingsTriangulationConfig += "clusterToDelLocationDistanceThreshold" -> Config.getProperty("pings-triangulation-configuration.clusterToDelLocationDistanceThreshold", "100")

    val sparkConfig = new SparkConf().setAppName(appName).setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.local_dc", datacenter)
      .set("spark.cassandra.auth.username", username)
      .set("spark.cassandra.auth.password", password)
      .set("spark.cassandra.connection.keep_alive_ms",(streamingInterval*60*1000 + 100).toString)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-delivery-location-refinement-executor.properties -Dcom.sun.management.jmxremote.port=25897 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")

    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sparkConfig, Minutes(streamingInterval))
    try {
      Logger.log(this.getClass, INFO, BaseSLog(s"Getting delivered data"))
      val refinedDeliveryDataRDD = ssc.cassandraTable("compass", "refined_delivered_data").select("date", "timestamp")
        .where("date = ? and timestamp >= ? and timestamp <= ?", DateUtils.dateFormatter.format(LocalDateTime.now()),
          LocalDateTime.now().minusHours(2).toInstant(ZoneOffset.ofHoursMinutes(5,30)).toEpochMilli,
          LocalDateTime.now().minusHours(1).toInstant(ZoneOffset.ofHoursMinutes(5,30)).toEpochMilli)
      val refinedDeliveryDataDStream: ConstantInputDStream[CassandraRow] = new ConstantInputDStream(ssc, refinedDeliveryDataRDD)
      DeliveryLocationRefinementService.streamDeliveryPings(ssc, refinedDeliveryDataDStream, pingsTriangulationConfig, clusterScoreConfig)
      ssc.start()
      ssc.awaitTermination()
    }
    catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing Delivery Location Refinement App: " + e.getMessage, e))
        throw e
    }
    finally {
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping Delivery Location Refinement App"))
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        Logger.log(this.getClass, ERROR, BaseSLog(s"Delivery Location Refinement App stopped"))
      }
    }
  }
}
