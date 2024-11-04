package org.apache.spark.streaming.pulsar

import java.util.Locale
import org.apache.pulsar.common.naming.TopicName
import org.apache.spark.internal.config.ConfigBuilder

// All options should be lowercase to simplify parameter matching
object PulsarContants {
  private[spark] val PARTITION = "-partition-"
  private[spark] val LEDGER_ENTRY_SEPARATOR = ":"
  private[spark] val LEDGER_OPENED_STATE = "LedgerOpened"

  // option key prefix for different modules
  private[spark] val PulsarClientOptionKeyPrefix: String = "pulsar.client."
  private[spark] val PulsarReaderOptionKeyPrefix: String = "pulsar.reader."


  // options
  val TopicSingle: String = "topicNames"
  val PartitionSuffix: String = TopicName.PARTITIONED_TOPIC_SUFFIX
  val TopicOptionKeys: Set[String] = Set(TopicSingle)
  val ServiceUrlOptionKey: String = "serviceUrl"
  val MaxRatePerPartitionOptionKey: String = "spark.streaming.maxRatePerPartition"
  val SubscriptionPrefix: String = "subscriptionPrefix"
  val PredefinedSubscription: String = "subscriptionName"
  val subscriptionInitialPositionKey: String = "subscriptionInitialPosition"
  val PollTimeoutMS: String = "pollTimeoutMs"
  val AuthPluginClassName: String = "authPluginClassName"
  val AuthParams: String = "authParams"
  val Authentication: String = "authentication"

  val FilteredKeys: Set[String] = Set(TopicSingle,
    ServiceUrlOptionKey,
    PredefinedSubscription,
    Authentication)

}

