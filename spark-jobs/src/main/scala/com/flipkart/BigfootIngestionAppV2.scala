package com.flipkart

import com.flipkart.config.Config
import com.flipkart.core.Schema
import com.flipkart.messaging.ViestiPipeline
import com.flipkart.service.{GeoTagBigfootServiceImpl, ViestiStreamingService}
import com.flipkart.utils.Constant.{VIESTI_CONSUMER_NAME, VIESTI_ENDPOINT, VIESTI_GEOTAG_SUBSCRIPTION_NAME, VIESTI_GEOTAG_TOPIC}
import com.flipkart.utils.Utility.authnConfigMap
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object BigfootIngestionAppV2 {

  def main(args: Array[String]) {

    lazy val sparkMaxCores = Config.getProperty("spark.BigfootIngestionAppV2.max.cores", "10")
    lazy val sparkExecutorMemory = Config.getProperty("spark.BigfootIngestionAppV2.executor.memory", "5g")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.75.123.103:7077,10.75.123.104:7077,10.75.123.105:7077")
    lazy val appName = Config.getProperty("spark.BigfootIngestionAppV2.appName", "Bigfoot Ingestion App V2")
    lazy val numPartitions = Config.getProperty("spark.BigfootIngestionAppV2.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.BigfootIngestionAppV2.repartition", "false")
    lazy val geotagSchemaName=Config.getProperty("spark.BigfootIngestionAppV2.geotag.schemaName","GeoTag_Viesti")
    lazy val geotagSchemaVesion=Config.getProperty("spark.BigfootIngestionAppV2.geotag.Version","1.0")
    lazy val maxRecieverRate=Config.getProperty("spark.BigfootIngestionAppV2.viestiReceiver.maxRate","200")



    lazy val streamingInterval = Config.getProperty("spark.BigfootIngestionAppV2.streaming.Interval", "60").toLong
    val sc = new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-bigfoot-ingestion-executor.properties -Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
      .set("spark.streaming.receiver.maxRate", maxRecieverRate)

    var viestiConfigMap = Map[String, String]()
    viestiConfigMap += VIESTI_ENDPOINT -> Config.getProperty("viestiConfig.endPoint", "pulsar://10.24.20.113:6650")
    viestiConfigMap += VIESTI_GEOTAG_TOPIC -> Config.getProperty("viestiConfig.geotag.topicName", "persistent://cl-discover/ekl-compass/geotag_feed")
    viestiConfigMap += VIESTI_GEOTAG_SUBSCRIPTION_NAME -> Config.getProperty("viestConfig.geotag.bigfoot.subscriptionName", "cl-discover/bigfoot")
    viestiConfigMap += VIESTI_CONSUMER_NAME -> Config.getProperty("viesti.geotag.bigfoot.consumerName","cl-discover-prod/bigfoot")

    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sc, Seconds(streamingInterval))
    try {
      val viestiBigfootGeotagStream = new ViestiPipeline(ssc, viestiConfigMap, authnConfigMap).getStream

      Logger.log(this.getClass, INFO, BaseSLog(s"saving geotag to bigfoot start"))
      ViestiStreamingService.processAndSaveToBigFoot(repartition.toBoolean, Integer.parseInt(numPartitions), viestiBigfootGeotagStream, Schema(geotagSchemaName, geotagSchemaVesion))(GeoTagBigfootServiceImpl)

      ssc.start()
      ssc.awaitTermination()
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing BigfootIngestionApp: " + e.getMessage, e))
        throw e
    }
    finally {
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping BigfootIngestionApp"))
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        Logger.log(this.getClass, ERROR, BaseSLog(s"BigfootIngestionApp stopped"))
      }
    }
  }
}
