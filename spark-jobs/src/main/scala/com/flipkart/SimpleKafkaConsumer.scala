
package com.flipkart;

import com.flipkart.config.Config
import com.flipkart.core.Schema
import com.flipkart.messaging.{KafkaPipeline, ZookeeperManager}
import com.flipkart.service.{EventBigfootServiceImpl, GeoTagBigfootServiceImpl, KafkaStreamingService}
import com.flipkart.utils.ConfigMapUtil
import com.flipkart.utils.ConfigMapUtil.configMap
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

object SimpleKafkaConsumer {

  def main(args: Array[String]) {

    lazy val sparkMaxCores = "5"
    lazy val sparkExecutorMemory = "1g"
    lazy val sparkMaster = "spark://localhost:7077"
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.75.123.103")
    lazy val appName =  "Simple kafak Consumer"
    lazy val kafkaBrokers = "localhost:9092"
    lazy val numPartitions = Config.getProperty("spark.StreamingApp.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.StreamingApp.repartition", "false")


    lazy val geotagSchema="GeoTag"
    lazy val eventSchema="Event"

    lazy val streamingInterval = "10".toLong
    val sc = new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-bigfoot-ingestion-executor.properties -Dcom.sun.management.jmxremote.port=25497 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")


    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sc, Durations.seconds(10))

    try {
      val kafkaBigfootGeotagStream = new KafkaPipeline(ssc, "geotag_feed", kafkaBrokers, ConfigMapUtil.zkPropertiesBigfoot).getStream


      Logger.log(this.getClass, INFO, BaseSLog(s"saving geotag to bigfoot start"))
        kafkaBigfootGeotagStream.foreachRDD(rdd => {
          parse(rdd)
          ZookeeperManager.updateOffsetsinZk(ConfigMapUtil.zkPropertiesBigfoot, rdd)
        })
      ssc.start()
      ssc.awaitTermination()
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing com.flipkart.BigfootIngestionApp: " + e.getMessage, e))
        throw e
    }
    finally {
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping com.flipkart.BigfootIngestionApp"))
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        Logger.log(this.getClass, ERROR, BaseSLog(s"com.flipkart.BigfootIngestionApp stopped"))
      }
    }
  }
  def parse(value: RDD[(String, String)]): Unit ={
    value.foreach(x=> {
      println("Key: "+x._1 +", Value:"+x._2)
    })
  }
}
