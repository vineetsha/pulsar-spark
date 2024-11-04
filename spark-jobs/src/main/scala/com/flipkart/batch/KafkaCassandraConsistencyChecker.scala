package com.flipkart.batch


import java.io.{BufferedWriter, FileWriter}
import java.text.SimpleDateFormat

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.driver.core.SimpleStatement
import com.datastax.spark.connector.cql.CassandraConnector
import com.flipkart.config.Config
import com.flipkart.core.{GeoTagPayload, SignaturePayload}
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.{GeoTagCassandraModel, SignatureCassandraModel}
import com.flipkart.utils.Mail.{Mail, send}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.{JsonUtility, Utility}
import kafka.serializer.StringDecoder
import org.apache.logging.log4j.Level._
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.mutable.ListBuffer
/**
 * Created by sharma.varun on 04/11/15.
 */

/*
command to run on stage
export APP_ENV="stage" && export CONFIG_SERVICE_ENDPOINT="config-service-sandbox.ch.flipkart.com" && export CONFIG_SERVICE_PORT="8080" && /usr/share/spark/bin/spark-submit --total-executor-cores 14 --supervise --driver-java-options "-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming.properties -Dcom.sun.management.jmxremote.port=25496 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true" --driver-class-path "/usr/share/spark/jars/fk-ekl-spark-jobs.jar" --conf spark.cores.max=30 --conf spark.executor.memory=10g --conf spark.driver.memory=10G --conf spark.cassandra.input.split.size_in_mb=67108864 --class "com.flipkart.batch.KafkaCassandraConsistencyChecker" /usr/share/spark/jars/fk-ekl-spark-jobs.jar "62d247c2"
 */
object KafkaCassandraConsistencyChecker {
  def checkInGeoTagTable(cassandraConnector: CassandraConnector, data2: GeoTagCassandraModel): Boolean = {
    val a = (data2.device_id, data2.date, data2.src, data2._type, data2.time, data2.tag_id)
    val count = cassandraConnector.withSessionDo { session =>
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
      val st = new SimpleStatement("select device_id from compass.geotag WHERE" +
        " device_id = ? and date=? and src =? and type =? AND time =? and tag_id=?",
        data2.device_id,
        data2.date,
        data2.src,
        data2._type,
        new java.lang.Long(dateFormat.parse(data2.time).getTime),
        data2.tag_id
      )
      session.execute(st)
    }.all().size()

    if (count != 1) {
      Logger.log(this.getClass, ERROR, BaseSLog(s"row count for key $a is :" + count))
      true
    }
    else {
      Logger.log(this.getClass, DEBUG, BaseSLog(s"row count for key $a is :" + count))
      false
    }
  }

  def checkInSignatureTable(cassandraConnector: CassandraConnector, data2: SignatureCassandraModel): Boolean = {
    val a = (data2.tag_id, data2.src, data2._type, data2.time)
    val count = cassandraConnector.withSessionDo { session =>
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
      val st = new SimpleStatement("select tag_id from compass.signature WHERE" +
        " tag_id=? and src =? and type =? AND time =?;",
        data2.tag_id,
        data2.src,
        data2._type,
        new java.lang.Long(dateFormat.parse(data2.time).getTime)
      )
      session.execute(st)
    }.all().size()

    if (count != 1) {
      Logger.log(this.getClass, ERROR, BaseSLog(s"row count for key $a is :" + count))
      true
    }
    else {
      Logger.log(this.getClass, INFO, BaseSLog(s"row count for key $a is :" + count))
      false
    }
  }

  def main(args: Array[String]) {
    var password:String = null
    if (args.length < 1) {
      System.err.println("Usage: KafkaCassandraConsisencyChecker <smtp:password>")
      System.exit(1)
    }
    else{
      password = args(0)
    }
    lazy val kafkaBrokers = Config.getProperty("spark.StreamingApp.kafkaBrokers", "ekl-lmp-1.stage.ch.flipkart.com:9092,ekl-lmp-2.stage.ch.flipkart.com:9092,ekl-lmp-3.stage.ch.flipkart.com:9092")
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.75.123.103")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.75.123.103:7077,10.75.123.104:7077,10.75.123.105:7077")

    var zkPropertiesCompass = Map[String, String]()
    zkPropertiesCompass += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.groupId", "compass")
    zkPropertiesCompass += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.ch.flipkart.com:2181")

    var zkPropertiesConsumer = Map[String, String]()
    zkPropertiesConsumer += "group.id" -> "compass_consistency_checker"
    zkPropertiesConsumer += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.ch.flipkart.com:2181")

    lazy val kafkaParams = Map("metadata.broker.list" -> kafkaBrokers)

    //  val kafkaBrokers = "ekl-compass-1.nm.flipkart.com:9092,ekl-compass-2.nm.flipkart.com:9092,ekl-compass-3.nm.flipkart.com:9092"
    //  zkProperties += "group.id" -> "compass"
    //  zkProperties += "zookeeper.connect" -> "compass-app-vm-0001.nm.flipkart.com:2181,compass-app-vm-0002.nm.flipkart.com:2181,compass-app-vm-0003.nm.flipkart.com:2181,compass-app-vm-0004.nm.flipkart.com:2181,compass-app-vm-0005.nm.flipkart.com:2181"

    val sc = new SparkContext(new SparkConf()
      .setAppName("KafkaCassandraConsistencyChecker")
      .setMaster(sparkMaster)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming-executor.properties -Dcom.sun.management.jmxremote.port=29497 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
    )
    val topics = Array("geotag_feed","signature_feed")

    val zkManagerCompass =new ZookeeperManager(zkPropertiesCompass).init()
    val zkManagerConsumer = new ZookeeperManager(zkPropertiesConsumer).init()
    val offsets = zkManagerCompass.kafkaRecorder.getOffsets(topics)

    val offsetRanges = new ListBuffer[OffsetRange]()
    offsets.foreach(x=>{
      val latestOffsetConsumer =zkManagerConsumer.kafkaRecorder.getOffset(x._1.topic, x._1.partition)
      if (x._2 > 0 && x._2 > latestOffsetConsumer) {
        offsetRanges += OffsetRange.create(x._1.topic, x._1.partition, latestOffsetConsumer, x._2)
      }
    })
    Logger.log(this.getClass, INFO, BaseSLog(s"Fetched Offsets:$offsetRanges"))

    val geoTagOffsetRanges = offsetRanges.filter(x=>x.topic=="geotag_feed")
    val signatureOffsetRanges = offsetRanges.filter(x=>x.topic=="signature_feed")

    val rddGeoTag = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParams, geoTagOffsetRanges.toArray)
    val rddSignature = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParams, signatureOffsetRanges.toArray)

    var exceptionsGeoTag:Array[(String,String,String,String,String,String)] = Array()
    var exceptionsSignature:Array[(String,String,String,String)] = Array()

    val cassandraConnector = CassandraConnector(sc.getConf)
    try {
      exceptionsGeoTag = rddGeoTag.repartitionByCassandraReplica("compass","geotag").map(x => JsonUtility.deserialize[GeoTagPayload](x._2).convertToCassandraModel()).filter(data => checkInGeoTagTable(cassandraConnector, data)).map(data2 => (data2.device_id, data2.date, data2.src, data2._type, data2.time, data2.tag_id)).collect()
      exceptionsSignature = rddSignature.repartition(20).map(x => JsonUtility.deserialize[SignaturePayload](x._2).convertToCassandraModel()).filter(data => checkInSignatureTable(cassandraConnector, data)).map(data2 => (data2.tag_id, data2.src, data2._type, data2.time)).collect()

      Logger.log(this.getClass, INFO, BaseSLog(s"Fetched GeoTag Exceptions:$exceptionsGeoTag"))
      Logger.log(this.getClass, INFO, BaseSLog(s"Fetched Signature Exceptions:$exceptionsSignature"))

      ZookeeperManager.updateOffsetsinZk(zkPropertiesConsumer, rddGeoTag)
      ZookeeperManager.updateOffsetsinZk(zkPropertiesConsumer, rddSignature)
    } catch {
      case e: Exception=>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in querying cassandra/updating offsets in zk: " + e.getMessage ,e))
    }
    //write to csv
    Logger.log(this.getClass, INFO, BaseSLog(s"Writing to csv exception_count.csv"))
    val out = new BufferedWriter(new FileWriter("exception_count.csv"))
    val writer = new CSVWriter(out)
    val exceptionsLineLimit = 10000
    if ((exceptionsGeoTag.length + exceptionsSignature.length) < exceptionsLineLimit) {
      writer.writeNext(Array("GeoTag Exceptions"))
      writer.writeNext(Array("device_id", "date", "src", "type", "time", "tag_id"))
      for (elem <- exceptionsGeoTag) {
        writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
      }
      writer.writeNext(Array("Signature Exceptions"))
      writer.writeNext(Array("tag_id", "src", "type", "time"))
      for (elem <- exceptionsSignature) {
        writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
      }
    } else{
      writer.writeNext(Array(s"Number of exceptions greater than $exceptionsLineLimit. So not logging all, look into issue ASAP"))
    }
    writer.close()
    Logger.log(this.getClass, INFO, BaseSLog(s"Done writing to csv exception_count.csv"))
    //send mail
    if (Utility.getEnvironmentVariable("APP_ENV").getOrElse("default").equals("nm")){
      Logger.log(this.getClass, INFO, BaseSLog(s"Sending mail"))
      send a new Mail(
        from = ("sharma.varun@flipkart.com", "Varun Sharma"),
        to = Seq("lm-geo-dev@flipkart.com"),
        cc = Seq("sharma.varun@flipkart.com"),
        subject =
          if ((exceptionsGeoTag.length + exceptionsSignature.length)>10000)
            "<Urgent><Issues>Compass Streaming Job Mismatch Report"
          else if((exceptionsGeoTag.length + exceptionsSignature.length)>0)
            "<Issues>Kafka Cassandra Mismatch Report"
          else
            "<No Issues>Kafka Cassandra Mismatch Report",
        message = "PFA the report....",
        attachment = Option(new java.io.File("exception_count.csv")),
        hostName = "10.33.102.104",
        username = "lm.geo",
        password = password
      )
    }
//    val offsetRanges = offsets.map(x=>{
//      val latestOffsetConsumer =zkManagerConsumer.kafkaRecorder.getOffset(x._1.topic, x._1.partition)
//      if (x._2 > 0 && x._2 > latestOffsetConsumer) {
//        OffsetRange.create(x._1.topic, x._1.partition, x._2-1, x._2)
//      }
//      else{
//        null
//      }
//    }).filter(x => x != null)
//
//
//    val partitions = 0 to 14
//    val offsetRanges = new ListBuffer[OffsetRange]()
//    topics.foreach(topic => {
//      partitions.foreach(partition => {
//        val latestOffsetCompass = zkManagerCompass.kafkaRecorder.getOffset(topic, partition)
//        val latestOffsetConsumer =zkManagerConsumer.kafkaRecorder.getOffset(topic, partition)
//        if (latestOffsetCompass > 0 && latestOffsetCompass > latestOffsetConsumer) {
//          val offsetRange = OffsetRange.create(topic, partition, latestOffsetCompass-1, latestOffsetCompass)
//          offsetRanges += offsetRange
//        }
//      })
//    })

//    manually update the offsets for first time
//    import kafka.common.TopicAndPartition
//    import org.apache.spark.streaming.kafka.HasOffsetRanges
//    val a= rddGeoTag.asInstanceOf[HasOffsetRanges].offsetRanges
//
//    // offsetRanges.length = # of Kafka partitions being consumed
//
//    val zookeeperOffsets = rddGeoTag.mapPartitionsWithIndex((i, partition) => {
//      val offsetRange: OffsetRange = a(i)
//      // get any needed data from the offset range
//      //      Logger.log(this.getClass,INFO,BaseSLog(s"ConsumptionPipeline PartitionLength=${partition.length}, fromOffset=${offsetRange.fromOffset}, untilOffset=${offsetRange.untilOffset}"))
//
//      if (offsetRange.untilOffset > offsetRange.fromOffset) {
//        Logger.log(this.getClass, INFO, BaseSLog(s"ConsumptionPipeline Kafka Commit Record [Topic=${offsetRange.topic}, PartitionId=${offsetRange.partition}, PartitionLength=${partition.length}, fromOffset=${offsetRange.fromOffset}, untilOffset=${offsetRange.untilOffset}"))
//        ZookeeperManager.get(zkPropertiesConsumer).kafkaRecorder.commitOffset(Map(TopicAndPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.untilOffset))
//        List(offsetRange).iterator
//      }
//      else {
//        List().iterator
//      }
//    }).collect().toList
//    if (zookeeperOffsets.nonEmpty) {
//      Logger.log(this.getClass, INFO, BaseSLog(s"ConsumptionPipeline Zookeeper Offset's Updated to $zookeeperOffsets"))
//    }

  }
}