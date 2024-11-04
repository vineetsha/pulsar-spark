package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.spark.streaming.pulsar.PulsarContants.PARTITION

private[pulsar] class TopicPartition(val topicName: String,val partition: Int)

private[pulsar]  case class PulsarPartitionMetadata(ledgers: Seq[PulsarLedger],
                                   backlog: Long,
                                   markDeletePosition: MessageIdImpl)

case class PulsarOffsetRange(startMessageId: MessageIdImpl,
                             endMessageId: MessageIdImpl,
                             batchSize: Int) extends Serializable

private[pulsar] object TopicPartition{
  def getPartitionName(topicPartition: TopicPartition):String={
    topicPartition.topicName+PARTITION+topicPartition.partition.toString
  }
  def getPartitionName(topicName: String, partition: Int): String={
    topicName+PARTITION+partition.toString
  }

  def getTopicName(topicPartitionName: String): String={
    topicPartitionName.substring(0, topicPartitionName.indexOf(PARTITION))
  }
}
