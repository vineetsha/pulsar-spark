package com.flipkart

import com.flipkart.config.Config
import com.flipkart.messaging.KafkaPipeline
import com.flipkart.service._
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by vikas.gargay on 25/10/16.
 */
object SmartLookupApp {

  def main(args: Array[String]){

    lazy val sparkMaxCores = Config.getProperty("spark.SmartLookupApp.max.cores", "10")
    lazy val sparkExecutorMemory = Config.getProperty("spark.StreamingApp.executor.memory", "5g")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.75.123.103:7077,10.75.123.104:7077,10.75.123.105:7077")
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.75.123.103")
    lazy val appName = Config.getProperty("spark.SmartLookupApp.appName", "Smart Lookup App")
    lazy val kafkaBrokers = Config.getProperty("spark.StreamingApp.kafkaBrokers", "ekl-lmp-1.stage.nm.flipkart.com:9092")
    lazy val numPartitions = Config.getProperty("spark.SmartLookupApp.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.SmartLookupApp.repartition","false")

    var zkProperties = Map[String, String]()
    zkProperties += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.smartlookup", "smartlookup")
    zkProperties += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.nm.flipkart.com:2181")

    var configMap = Map[String, String]()
    configMap += "userGeoCodingMsgQUrl" -> Config.getProperty("UserGeoCoding.Url", "")
    configMap += "mmiGeoCodingMsgQUrl" -> Config.getProperty("MmiGeoCoding.Url","")

    lazy val streamingInterval = Config.getProperty("SmartLookup.App.Interval","10").toLong
    lazy val maxRatePerPartition = Config.getProperty("SmartLookup.App.maxRatePerPartition", "100").toLong

    val sc = new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.keep_alive_ms",(streamingInterval*1000 + 100).toString)
      .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition.toString)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-smart-lookup-executor.properties -Dcom.sun.management.jmxremote.port=25697 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")

    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sc, Seconds(streamingInterval))
    try {
      val kafkaStringStreamGeotag = new KafkaPipeline(ssc, "geotag_feed", kafkaBrokers, zkProperties).getStream

      SmartAddressBuildService.saveDataToCassandra(repartition.toBoolean, Integer.parseInt(numPartitions), kafkaStringStreamGeotag, zkProperties)

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
