package com.flipkart


import com.flipkart.config.Config
import com.flipkart.core.Schema
import com.flipkart.messaging.KafkaPipeline
import com.flipkart.service.{EventBigfootServiceImpl, GeoCodeService, GeoTagBigfootServiceImpl, KafkaStreamingService}
import com.flipkart.utils.ConfigMapUtil
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BigfootIngestionApp {

  def main(args: Array[String]) {

    lazy val sparkMaxCores = Config.getProperty("spark.StreamingApp.max.cores", "10")
    lazy val sparkExecutorMemory = Config.getProperty("spark.StreamingApp.executor.memory", "5g")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.75.123.103:7077,10.75.123.104:7077,10.75.123.105:7077")
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.75.123.103")
    lazy val appName = Config.getProperty("spark.BigfootIngestionApp.appName", "Bigfoot Ingestion App")
    lazy val kafkaBrokers = Config.getProperty("spark.StreamingApp.kafkaBrokers", "ekl-lmp-1.stage.ch.flipkart.com:9092")
    lazy val numPartitions = Config.getProperty("spark.StreamingApp.numPartitions", "50")
    lazy val repartition = Config.getProperty("spark.StreamingApp.repartition", "false")

    var configMap = Map[String, String]()
    configMap += "bigfootDartEndpoint" -> Config.getProperty("Bigfoot.Dart.BatchIngestion.Url", "http://10.47.2.124:28223/ingestion/events/wsr/scp/ekl")
    configMap += "geotagSchemaVersion" -> Config.getProperty("Bigfoot.GeoTag.SchemaVersion","5.0")
    configMap += "eventSchemaVersion" -> Config.getProperty("Bigfoot.Event.SchemaVersion","1.0")
    configMap += "bigfootGroupAmount" -> Config.getProperty("Bigfoot.Payload.Group.Amount", "200")
    configMap += "varadhiUrl" -> Config.getProperty("Varadhi.Ingestion.Url", "10.65.38.218")
    configMap += "fsdExternalEndPoint" -> Config.getProperty("fsd.external.Url", "http://flo-fkl-app2.stage.ch.flipkart.com:27012/fsd-external-apis")
    configMap += "facilityEndPoint" -> Config.getProperty("facility.Url", "http://flo-fkl-app5.stage.ch.flipkart.com:27747")
    configMap += "userGeoCodingMsgQUrl" -> Config.getProperty("UserGeoCoding.MsgQ.url","")
    configMap += "userGeoCodingServiceUrl" -> Config.getProperty("UserGeoCoding.Service.url","")
    configMap += "mmiGeoCodingMsgQUrl" -> Config.getProperty("MmiGeoCoding.MsgQ.Url","")
    configMap += "mmiGeoCodingServiceUrl" -> Config.getProperty("MmiGeoCoding.Service.Url","")
    configMap += "cassandraHost" -> Config.getProperty("spark.StreamingApp.cassandraHost","").split(",").apply(0)
    configMap += "geoCoderEndPoint" -> Config.getProperty("geoCoder.Geocode.Url", "http://ekl-lmp-1.stage.nm.flipkart.com:5555")

    lazy val geotagSchema="GeoTag"
    lazy val eventSchema="Event"

    var zkPropertiesGeoCode = Map[String, String]()
    zkPropertiesGeoCode += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.geoCode", "geoCode")
    zkPropertiesGeoCode += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.ch.flipkart.com:2181")

    var zkPropertiesGeoCodevalidation = Map[String, String]()
    zkPropertiesGeoCodevalidation += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.geoCodeValidation", "geoCode")
    zkPropertiesGeoCodevalidation += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.ch.flipkart.com:2181")

    lazy val streamingInterval = Config.getProperty("Bigfoot.App.Interval", "60").toLong
    val sc = new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.keep_alive_ms", (streamingInterval * 1000 + 100).toString)
      .set("spark.streaming.unpersist", "true")
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-bigfoot-ingestion-executor.properties -Dcom.sun.management.jmxremote.port=25497 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")


    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Streaming Context of app: $appName"))
    val ssc = new StreamingContext(sc, Seconds(streamingInterval))

    try {
      val kafkaBigfootGeotagStream = new KafkaPipeline(ssc, "geotag_feed", kafkaBrokers, ConfigMapUtil.zkPropertiesBigfoot).getStream
      val kafkaBigfootEventStream = new KafkaPipeline(ssc, "event_feed", kafkaBrokers, ConfigMapUtil.zkPropertiesBigfoot).getStream


      Logger.log(this.getClass, INFO, BaseSLog(s"saving geotag to bigfoot start"))
      KafkaStreamingService.processAndSaveToBigFoot(repartition.toBoolean,Integer.parseInt(numPartitions),kafkaBigfootGeotagStream,Schema(geotagSchema,configMap.apply("geotagSchemaVersion")))(GeoTagBigfootServiceImpl)

      Logger.log(this.getClass, INFO, BaseSLog(s"saving event to bigfoot start"))
      KafkaStreamingService.processAndSaveToBigFoot(repartition.toBoolean,Integer.parseInt(numPartitions),kafkaBigfootEventStream,Schema(eventSchema,configMap.apply("eventSchemaVersion")))(EventBigfootServiceImpl)

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
