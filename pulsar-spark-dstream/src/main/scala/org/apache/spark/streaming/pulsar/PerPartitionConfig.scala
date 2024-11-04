package org.apache.spark.streaming.pulsar

import org.apache.spark.SparkConf

abstract class PerPartitionConfig extends Serializable {
  /**
   *  Maximum rate (number of records per second) at which data will be read
   *  from each Pulsar partition.
   */
  def maxRatePerPartition(pulsarPartition: TopicPartition): Int
  def minRatePerPartition(pulsarPartition: TopicPartition): Int = 1
  def maxBytesPerPartition(pulsarPartition: TopicPartition): Int = 134217728
  def avgMsgsPerEntry(pulsarPartition: TopicPartition): Double
}
/**
 * Default per-partition configuration
 */
class DefaultPerPartitionConfig(conf: SparkConf)
  extends PerPartitionConfig {
  val maxRate = conf.get(PULSAR_MAX_RATE_PER_PARTITION)
  val minRate = conf.get(PULSAR_MIN_RATE_PER_PARTITION)
  val maxBytes = conf.get(PULSAR_MAX_BYTE_PER_PARTITION)
  val msgsPerEntry = conf.get(PULSAR_AVG_MESSAGE_PER_ENTRY)

  /**
   * Maximum rate (number of records per second) at which data will be read
   * from each Pulsar partition.
   */
  override def maxRatePerPartition(pulsarPartition: TopicPartition): Int = maxRate

  override def minRatePerPartition(pulsarPartition: TopicPartition): Int = minRate

  /**
   *  Maximum Bytes (number of bytes per second) at which data will be read
   *  from each Pulsar partition.
   */
  override def maxBytesPerPartition(pulsarPartition: TopicPartition): Int= maxBytes

  override def avgMsgsPerEntry(pulsarPartition: TopicPartition): Double = msgsPerEntry
}