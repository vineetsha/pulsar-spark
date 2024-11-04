package com.flipkart.messaging

/**
 * Created by sharma.varun
 */

import java.util.Properties


import com.flipkart.utils.logging.{BaseSLog, Logger}
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

import scala.collection.{Map, immutable, mutable}

sealed class ZookeeperManager(params: Map[String, String]) {

  private val AUTO_OFFSET_COMMIT = "auto.commit.enable"

  /** zkClient to connect to Zookeeper to commit the offsets. */
  private var zkClient: ZkClient = null

  var kafkaRecorder: KafkaZookeeperRecorder = null

  def init(): ZookeeperManager = {

    val props = new Properties()
    params.foreach(param => props.put(param._1, param._2))
    // Manually set "auto.commit.enable" to "false" no matter user explicitly set it to true,
    // we have to make sure this property is set to false to turn off auto commit mechanism in
    // Kafka.
    props.setProperty(AUTO_OFFSET_COMMIT, "false")
    val consumerConfig = new ConsumerConfig(props)
    Logger.log(this.getClass,INFO,BaseSLog(s"Connecting to Zookeeper: ${consumerConfig.zkConnect}"))
    zkClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs, consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)
    kafkaRecorder = new KafkaZookeeperRecorder

    this
  }


  class KafkaZookeeperRecorder {

    private val groupId = params("group.id")

    /**
     * Commit the offset of Kafka's topic/partition, the commit mechanism follow Kafka 0.8.x's
     * metadata schema in Zookeeper.
     */
    def commitOffset(offsetMap: Map[TopicAndPartition, Long]): Unit = {
      if (zkClient == null) {
        throw new IllegalStateException("Zookeeper client is unexpectedly null")
      }

      for ((topicAndPart, offset) <- offsetMap) {
        try {
          val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPart.topic)
          val zkPath = s"${topicDirs.consumerOffsetDir}/${topicAndPart.partition}"

          ZkUtils.updatePersistentPath(zkClient, zkPath, offset.toString)
        } catch {
          case e: Exception =>
            Logger.log(this.getClass,ERROR,BaseSLog(s"Exception during commit offset $offset for topic" +
              s"${topicAndPart.topic}, partition ${topicAndPart.partition}", e))
        }
        Logger.log(this.getClass,INFO,BaseSLog(s"Committed offset $offset for topic ${topicAndPart.topic}, partition ${topicAndPart.partition}"))
      }
    }

    def getOffsets(topics: Seq[String]): immutable.Map[TopicAndPartition, Long] = {
      val partitions = ZkUtils.getPartitionsForTopics(zkClient, topics)
      val topicAndPartitionOffSetMap = mutable.Map[TopicAndPartition, Long]()

      partitions.foreach(topicAndPart => {
        val currentTopicName = topicAndPart._1
        val topicDirs = new ZKGroupTopicDirs(groupId, currentTopicName)
        topicAndPart._2.foreach(partitionId => {

          val zkPath = s"${topicDirs.consumerOffsetDir}/$partitionId"
          val checkPoint = ZkUtils.readDataMaybeNull(zkClient, zkPath)._1 match {
            case None => 0L
            case Some(x) => x.toLong
          }

          val topicAndPartition = TopicAndPartition(currentTopicName, partitionId)
          topicAndPartitionOffSetMap.put(topicAndPartition, checkPoint)
        })
      })

      topicAndPartitionOffSetMap.toMap
    }

    def getOffset(topic:String, partition: Int):Long = {
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
      val checkPoint = ZkUtils.readDataMaybeNull(zkClient, zkPath)._1 match {
        case None => 0L
        case Some(x) => x.toLong
      }
      checkPoint
      }
  }
}


object ZookeeperManager {

  var zkManager:Map[String,ZookeeperManager] = null

  private def checkForConsistency(properties:Map[String,String],offsetRange: OffsetRange):Boolean={
    offsetRange.untilOffset > offsetRange.fromOffset && {
      var currentOffset= ZookeeperManager.get(properties).kafkaRecorder.getOffset(offsetRange.topic,offsetRange.partition)
      //if it is the first time we are consuming from this topic and partition, dont throw exception
      if(currentOffset == 0){
        currentOffset = offsetRange.fromOffset
      }
      if (currentOffset != offsetRange.fromOffset) {
        Logger.log(this.getClass, INFO, BaseSLog(s"ConsumptionPipeline Fetching currentOffset from Zookeeper [CurrentOffset=$currentOffset"))
        Logger.log(this.getClass, ERROR, BaseSLog(s"ConsumptionPipeline kafka offsets not consistent [Topic=${offsetRange.topic}, PartitionId=${offsetRange.partition}, fromOffset=${offsetRange.fromOffset}, untilOffset=${offsetRange.untilOffset}"))
        throw new RuntimeException("Inconsistency in zk offsets")
      }
      currentOffset == offsetRange.fromOffset
    }
  }

  def get(properties: Map[String,String]) = {
    val groupId = properties.get("group.id").get
    if (zkManager == null) {
      zkManager = Map[String, ZookeeperManager]()
    }
    if (!zkManager.contains(groupId)) {
      zkManager += groupId -> new ZookeeperManager(properties).init()
    }
    zkManager.get(groupId).get
  }

  def updateOffsetsinZk(properties: Map[String,String], rdd:RDD[(String,String)]): Unit= {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // offsetRanges.length = # of Kafka partitions being consumed

    val zookeeperOffsets = rdd.mapPartitionsWithIndex((i, partition) => {
      val offsetRange: OffsetRange = offsetRanges(i)
      // get any needed data from the offset range
      //      Logger.log(this.getClass,INFO,BaseSLog(s"ConsumptionPipeline PartitionLength=${partition.length}, fromOffset=${offsetRange.fromOffset}, untilOffset=${offsetRange.untilOffset}"))

      if (checkForConsistency(properties,offsetRange)) {
        Logger.log(this.getClass, INFO, BaseSLog(s"ConsumptionPipeline Kafka Commit Record [Topic=${offsetRange.topic}, PartitionId=${offsetRange.partition}, PartitionLength=${partition.length}, fromOffset=${offsetRange.fromOffset}, untilOffset=${offsetRange.untilOffset}"))
        ZookeeperManager.get(properties).kafkaRecorder.commitOffset(Map(TopicAndPartition(offsetRange.topic, offsetRange.partition) -> offsetRange.untilOffset))
        List(offsetRange).iterator
      }
      else {
        List().iterator
      }
    }).collect().toList
    if (zookeeperOffsets.nonEmpty) {
      Logger.log(this.getClass, INFO, BaseSLog(s"ConsumptionPipeline Zookeeper Offset's Updated to $zookeeperOffsets"))
    }
  }

  def updateOffsetsinZk(properties: Map[String,String], dstream:DStream[(String,String)]): Unit={
    dstream.foreachRDD(rdd=> {
      updateOffsetsinZk(properties,rdd)
    })
  }
}

