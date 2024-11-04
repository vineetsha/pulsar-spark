package com.flipkart.messaging

import _root_.kafka.message.MessageAndMetadata
import _root_.kafka.serializer.StringDecoder
import com.flipkart.utils.logging.{BaseSLog, Logger}
import consumer.kafka.ReceiverLauncher
import kafka.utils.ZkUtils
import org.apache.logging.log4j.Level._
import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils


/**
 * Created by sharma.varun
 */
class KafkaPipeline(ssc: StreamingContext, topics: String, brokers: String, zkProperties:Map[String,String]) extends Serializable {

  lazy val topicSet = topics.split(',').toSet
  lazy val kafkaParams = Map("metadata.broker.list" -> brokers, "auto.offset.reset"->"largest")
  val kafkaMessageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
  val numberOfReceivers = 3
  val streamingContext = ssc
  def getStream: DStream[(String, String)] = {
    Logger.log(this.getClass,INFO,BaseSLog(message="Initiating KafkaPipeline[%s]... Topics [%s], Brokers [%s]".format(hashCode(), topics, brokers),tag="CONSUMER"))

    val topicAndPartitionWithOffset = ZookeeperManager.get(zkProperties).kafkaRecorder.getOffsets(topicSet.toSeq)
    val offsetDistinctValues = topicAndPartitionWithOffset.values.toList.distinct

    Logger.log(this.getClass,INFO,BaseSLog(tag="CONSUMER",message=s"KafkaPipeline topicAndPartitionWithOffset for [$topics] from Zookeeper : $topicAndPartitionWithOffset"))
    //if it is the first time we are consuming from this topic start from largest offset
    if (topicAndPartitionWithOffset.isEmpty || (offsetDistinctValues.length == 1 && offsetDistinctValues.head == 0)) {
      Logger.log(this.getClass, INFO, BaseSLog(tag = "CONSUMER", message = "KafkaPipeline Starting with Latest Offsets."))
      lazy val kafkaParams = Map("metadata.broker.list" -> brokers, "auto.offset.reset"->"largest")
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    } else {
      Logger.log(this.getClass, INFO, BaseSLog(tag = "CONSUMER", message = "KafkaPipeline Starting with Offset From Zookeeper"))
      lazy val kafkaParams = Map("metadata.broker.list" -> brokers)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, topicAndPartitionWithOffset, kafkaMessageHandler)
    }

  }

  @Experimental
  def getReceiverBasedStream: DStream[(String,String)] = {
    val topicsToNumPartitionsMap = topicSet.toSeq.map(x=>{
      x -> numberOfReceivers
    }).toMap
    KafkaUtils.createStream(ssc, zkProperties.get("zookeeper.connect").get, zkProperties.get("group.id").get, topicsToNumPartitionsMap)
  }

  @Experimental
  def getKafkaSparkConsumerStream: DStream[(String, String)] = {
    val topic = topics.split(",").head
    val zkhosts = zkProperties.get("zookeeper.connect").get.split(",").map(x=> x.split(":").head).mkString(",")
    val zkports = "2181"
    val brokerPath = "/brokers"

    val kafkaProperties: Map[String, String] = Map("zookeeper.hosts" -> zkhosts,
      "zookeeper.port" -> zkports,
      "zookeeper.broker.path" -> brokerPath ,
      "kafka.topic" -> topic,
      "zookeeper.consumer.connection" -> zkProperties.get("zookeeper.connect").get,
      "zookeeper.consumer.path" -> ZkUtils.ConsumersPath,
      "kafka.consumer.id" -> zkProperties.get("group.id").get
    )

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}

    val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_AND_DISK_SER_2)

    tmp_stream.map(x=>{
      (x.getTopic,new String(x.getPayload))
    })
  }
}
