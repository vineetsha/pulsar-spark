package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.{Message, MessageId}

import scala.collection.JavaConverters.mapAsScalaMapConverter

// Wrapper class for PubSub Message since Message is not Serializable
class SparkPulsarMessage[T](val key: String,
                            val data: Array[Byte],
                            val value: T,
                            val topicName: String,
                            val messageId: MessageId,
                            val properties: Map[String, String],
                            val publishTime: Long, // Application Publish time
                            val eventTime: Long) extends Serializable

object SparkPulsarMessage {
  def apply[T](m: Message[T]): SparkPulsarMessage[T] = {
    val messageId = m.getMessageId
    new SparkPulsarMessage[T](m.getKey, m.getData, m.getValue,m.getTopicName,messageId, m.getProperties.asScala.toMap, m.getPublishTime, m.getEventTime)
  }
}
