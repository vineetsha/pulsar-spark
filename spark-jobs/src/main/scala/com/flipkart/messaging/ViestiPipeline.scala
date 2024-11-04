
package com.flipkart.messaging

import com.flipkart.utils.Constant.{VIESTI_CONSUMER_NAME, VIESTI_ENDPOINT, VIESTI_GEOTAG_SUBSCRIPTION_NAME, VIESTI_GEOTAG_TOPIC}
import com.flipkart.utils.Utility.getAuthentication
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pulsar.{SparkPulsarMessage, SparkStreamingReliablePulsarReceiver}


class ViestiPipeline(ssc: StreamingContext, viestConfig: Map[String, String], authnConfig: Map[String, String]) extends Serializable {
  private val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  private val topics = viestConfig.get(VIESTI_GEOTAG_TOPIC).get
  private val topicList = topics.split(',').toList
  private val subscriptionName: String = viestConfig.get(VIESTI_GEOTAG_SUBSCRIPTION_NAME).get
  private val consumerName = viestConfig.get(VIESTI_CONSUMER_NAME).get
  val TOPIC_NAMES = "topicNames"
  val SUBS_NAME = "subscriptionName"
  val SUBS_TYPE = "subscriptionType"


  def getStream: DStream[SparkPulsarMessage] = {
    Logger.log(this.getClass, INFO, BaseSLog(message = "Initiating ViestiPipeline[%s]... Topics [%s]".format(hashCode(), topics), tag = "CONSUMER"))
    val serviceUrl = viestConfig.get(VIESTI_ENDPOINT).get

    val confMap: Map[String, Any] = Map(
      TOPIC_NAMES -> topicList,
      SUBS_NAME -> subscriptionName,
      SUBS_TYPE -> SubscriptionType.Shared
    )


    val stream = new SparkStreamingReliablePulsarReceiver(
      storageLevel, // Storage level for spark streaming. Store on disk, memory etc
      serviceUrl, // Pulsar endpoint to connect to
      confMap, // Info about topics and subscription name
      getAuthentication(authnConfig), // Client credentials
      ssc.sparkContext.getConf, // passing on spark conf
      1000, // Max message that can be held in the consumer queue.
      consumerName, // Consumer name. in case you start more than one consumer in the subscription
      // This has no impact and is only helpful for debugging as logs would have this
      true, // Auto acknowledge for the read messages. Mark false if you want to manage
      // offsets yourself (by reading from rdd and maintaining offline)
      1.0D // Rate multiplier factor in case you want to have more rate than what
      // PIDRateEstimator suggests. > 1.0  is useful in case
      // of spark dynamic allocation to utilise extra resources.
      // Keep it 1.0 if spark dynamic allocation is disabled.
    )

    ssc.receiverStream(stream)

  }


}
