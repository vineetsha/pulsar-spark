package org.apache.spark.streaming

import org.apache.spark.internal.config.ConfigBuilder

package object pulsar {
  private[spark] val PULSAR_MAX_RATE_PER_PARTITION =
    ConfigBuilder("spark.streaming.pulsar.maxRatePerPartition")
      .version("3.1.2")
      .intConf
      .createWithDefault(1000)

  private[spark] val PULSAR_AUTO_ACK_ENABLED =
    ConfigBuilder("spark.streaming.pulsar.autoAck.enabled")
      .version("3.1.2")
      .booleanConf
      .createWithDefault(false)

  private[spark] val PULSAR_MIN_RATE_PER_PARTITION =
    ConfigBuilder("spark.streaming.pulsar.minRatePerPartition")
      .version("3.1.2")
      .intConf
      .createWithDefault(1)

  private[spark] val PULSAR_MAX_BYTE_PER_PARTITION =
    ConfigBuilder("spark.streaming.pulsar.maxByteRatePerPartition")
      .version("3.1.2")
      .intConf
      .createWithDefault(134217728)

  private[spark] val PULSAR_RDD_CHECKPOINTING_ENABLED =
    ConfigBuilder("spark.streaming.pulsar.rddCheckpointing.enabled")
      .version("3.1.2")
      .booleanConf
      .createWithDefault(false)

  private[spark] val PULSAR_AVG_MESSAGE_PER_ENTRY =
    ConfigBuilder("spark.streaming.pulsar.avgMsgPerEntry")
      .version("3.1.2")
      .doubleConf
      .createWithDefault(1.0)

}
