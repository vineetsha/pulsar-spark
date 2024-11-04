package com.flipkart

import com.flipkart.config.Config
import com.flipkart.core.CryptexInitializer
import com.flipkart.messaging.ViestiPipeline
import com.flipkart.service._
import com.flipkart.utils.Constant._
import com.flipkart.utils.Utility.{authnConfigMap, getCassandraConnectionAliveTime}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

import java.time.LocalDate

object StreamingAppV3 {

  def main(args: Array[String]) {

    lazy val sparkMaxCores = Config.getProperty("spark.streamingAppV3.max.cores", "20")
    lazy val sparkExecutorMemory = Config.getProperty("spark.streamingAppV3.executor.memory", "5g")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.52.18.196:7077,10.51.114.181:7077")
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.52.178.198,10.52.178.198,10.50.226.208,10.51.130.240,10.52.210.209,10.50.178.228,10.50.34.232,10.52.130.185")
    lazy val datacenter = Config.getProperty("spark.cassandra.connection.local_dc", "datacenter1")
    lazy val username = Config.getProperty("spark.cassandra.auth.username", "cassandra")
    lazy val cryptexInitializer = new CryptexInitializer()
    lazy val maxRecieverRate = Config.getProperty("spark.streaming.viestiReceiver.maxRate", "200")
    lazy val password = if (cryptexInitializer.isCryptexEnabled) cryptexInitializer.decrypt(
      Config.getProperty("spark.cassandra.auth.password", "cassandra"))
    else Config.getProperty("spark.cassandra.auth.password", "cassandra")
    lazy val appName = Config.getProperty("spark.StreamingAppV3.appName", "Viesti Compass Streaming")
    lazy val numPartitions = Config.getProperty("spark.StreamingAppV3.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.StreamingAppV3.repartition", "false")


    var clusterScoreConfig = Map[String, String]()
    clusterScoreConfig += DISTANCE_THRESHOLD -> Config.getProperty("cluster-score-configuration.distanceThreshold", "200")
    clusterScoreConfig += MIN_POINTS_COUNT -> Config.getProperty("cluster-score-configuration.minPointsCount", "3")
    clusterScoreConfig += MAJORITY_PERCENTAGE -> Config.getProperty("cluster-score-configuration.majorityPercentage", "50")
    clusterScoreConfig += DRIFT_PERCENTAGE -> Config.getProperty("cluster-score-configuration.driftThreshold", "200")


    var viestiConfigMap = Map[String, String]()
    viestiConfigMap += VIESTI_ENDPOINT -> Config.getProperty("viestiConfig.endPoint", "pulsar://10.24.20.113:6650")
    viestiConfigMap += VIESTI_GEOTAG_TOPIC -> Config.getProperty("viestiConfig.geotag.topicName", "persistent://cl-discover/ekl-compass/geotag_feed")
    viestiConfigMap += VIESTI_GEOTAG_SUBSCRIPTION_NAME -> Config.getProperty("viestConfig.geotag.subscriptionName", "cl-discover-prod/Shared")
    viestiConfigMap += VIESTI_CONSUMER_NAME -> Config.getProperty("viestiConfig.geotag.streaming.consumerName", "cl-discover-prod/StreamingAppV3")
    lazy val streamingInterval = Config.getProperty("streamingAppV3.streaming.interval", "60").toLong
    val sparkConfig = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.local_dc", datacenter)
      .set("spark.cassandra.auth.username", username)
      .set("spark.cassandra.auth.password", password)
      .set("spark.cassandra.connection.keep_alive_ms", getCassandraConnectionAliveTime(streamingInterval))
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-viesti-compass-streaming-executor.properties -Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false""")
      .set("spark.logConf", "true")
      .set("spark.streaming.receiver.maxRate", maxRecieverRate)

    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sparkConfig, Seconds(streamingInterval))
    try {
      val streamGeoTag = new ViestiPipeline(ssc, viestiConfigMap, authnConfigMap).getStream

      Logger.log(this.getClass, INFO, BaseSLog(s"Starting Data Ingestion"))
      ViestiStreamingService.processAndSaveToCassandra(repartition.toBoolean, numPartitions.toInt, streamGeoTag, clusterScoreConfig, ssc)

      ssc.start()
      ssc.awaitTermination()
    }
    catch {
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
