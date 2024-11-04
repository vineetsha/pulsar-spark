package com.flipkart

import com.flipkart.messaging.KafkaPipeline
import com.flipkart.service.PingsIngestionService
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.flipkart.config.Config
import com.flipkart.core.CryptexInitializer

object GpsPingsStreamingApp {


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
    lazy val appName = Config.getProperty("spark.GpsPingsStreamingApp.appName", "GPS Pings Streaming App")
    lazy val kafkaBrokers = Config.getProperty("spark.StreamingApp.kafkaBrokers", "10.49.66.142:9092,10.50.2.199:9092,10.50.210.208:9092")
    lazy val numPartitions = Config.getProperty("spark.StreamingApp.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.StreamingApp.repartition","false")
    lazy val kafkaMaxRatePerPartition = Config.getProperty("spark.streaming.kafka.maxRatePerPartition","10")

    var zkPropertiesGpsPings = Map[String, String]()
    zkPropertiesGpsPings += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.gps_pings", "gps_pings")
    zkPropertiesGpsPings += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "10.49.178.139:2181,10.50.18.132:2181,10.49.146.132:2181,10.51.66.163:2181,10.49.50.134:2181")
    lazy val streamingInterval = Config.getProperty("Compass.App.Interval","10").toLong

    val sparkConfig = new SparkConf().setAppName(appName).setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.local_dc", datacenter)
      .set("spark.cassandra.auth.username", username)
      .set("spark.cassandra.auth.password", password)
      .set("spark.cassandra.connection.keep_alive_ms",(streamingInterval*1000 + 100).toString)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-gps-pings-ingestion-executor.properties -Dcom.sun.management.jmxremote.port=25997 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", kafkaMaxRatePerPartition)

    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sparkConfig, Seconds(streamingInterval))
    try {
      val kafkaStringStreamGps = new KafkaPipeline(ssc, "gps_pings_feed", kafkaBrokers, zkPropertiesGpsPings).getStream
      Logger.log(this.getClass, INFO, BaseSLog(s"Starting Pings Streaming"))
      PingsIngestionService.saveDataToCassandra(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamGps, zkPropertiesGpsPings, ssc)
      ssc.start()
      ssc.awaitTermination()
    }
    catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing GPS Pings Streaming App: " + e.getMessage, e))
        throw e
    }
    finally {
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping GPS Pings Streaming App"))
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        Logger.log(this.getClass, ERROR, BaseSLog(s"GPS Pings Streaming App stopped"))
      }
    }
  }
}