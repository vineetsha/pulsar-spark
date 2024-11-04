package org.apache.spark.streaming.pulsar

import org.apache.pulsar.common.naming.TopicName
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.storage.BlockManager
import org.apache.spark.{HashPartitioner, Partition, Partitioner, SparkContext, SparkEnv, TaskContext}

import java.{util => ju}
import scala.collection.mutable.ListBuffer

private[spark] class SparkPulsarRDD[T](sc: SparkContext,
                        val clientParams: ju.Map[String, AnyRef],
                        val readerParams: ju.Map[String, AnyRef],
                        val pulsarPartitions: Array[PulsarPartition],
                        val consumerStatsMetricMap: Map[String, TopicConsumerMetrics]
                       )
  extends RDD[SparkPulsarMessage[T]](sc, Nil) with Logging {
  private val autoAckEnabled = sc.conf.get(PULSAR_AUTO_ACK_ENABLED)
  // Added hash based partitioner in case of checkpointing is enabled. This is required to create RDD from checkpoints in case of recovery. If not added it throws error saying: 'Partitioner file not found'
  override val partitioner: Option[Partitioner] = Some(new HashPartitioner(pulsarPartitions.length))

  /*
    Will be called from executors
    Returns: Iterator to List of the Messages
   */
  override def compute(split: Partition, context: TaskContext): Iterator[SparkPulsarMessage[T]] = {

    val sparkPulsarPartition = split.asInstanceOf[SparkPulsarPartition] //Convert passed Spark partition to spark pulsar partition
    logDebug(s"Creating consumer for partition: ${split.index} Range[ Starting offset: " + sparkPulsarPartition.pulsarPartition.pulsarOffsetRange.startMessageId + " ending Offset:" + sparkPulsarPartition.pulsarPartition.pulsarOffsetRange.endMessageId + "]")
    val consumer = new PulsarBoundedConsumer[T](sparkPulsarPartition.pulsarPartition,
      clientParams,
      readerParams,
      consumerStatsMetricMap.get(sparkPulsarPartition.pulsarPartition.topic).getOrElse(throw new IllegalStateException(s"Metric not defined for partition ${sparkPulsarPartition.pulsarPartition.topic}")),
      autoAckEnabled)

    // Adding task completion listener to close consumer after task completition
    context.addTaskCompletionListener(consumer)
    consumer.internalIterator.map(m => SparkPulsarMessage(m))
  }

  /*
  Here the RDD partitions are basically Pulsar Partitions
   */
  override protected def getPartitions: Array[Partition] = {
    val sortedPulsarPartition = pulsarPartitions.sortBy(p => TopicName.getPartitionIndex(p.topic))
    val partitionList: ListBuffer[Partition] = ListBuffer[Partition]()
    var index = 0
    for (partition <- sortedPulsarPartition) {
      partitionList += SparkPulsarPartition(index, partition)
      index += 1
    }
    partitionList.toArray
  }


  /*
  Returns the entries to be fetched in a batch.
   */
  override def count(): Long = {
    pulsarPartitions.map(_.pulsarOffsetRange.batchSize).sum
  }


  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    // The intention is best-effort consistent executor for a given topicpartition,
    // so that caching consumers can be effective.
    // TODO what about hosts specified by ip vs name
    val part = thePart.asInstanceOf[SparkPulsarPartition]

    //Extracting out partition index only from topic
    val tp = TopicName.getPartitionIndex(part.pulsarPartition.topic)
    // to get pref executor

    val sortedExecutors = getSortedExecutorList(SparkEnv.get.blockManager)
    val numExecutors = sortedExecutors.length
    if (numExecutors > 0) {
      // This allows cached PulsarClient in the executors to be re-used to read the same
      // partition in every batch.
      Seq(sortedExecutors(Math.floorMod(tp, numExecutors)))
    } else Seq.empty

  }

  private def getSortedExecutorList(blockManager: BlockManager): Array[String] = {
    blockManager.master
      .getPeers(blockManager.blockManagerId)
      .toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  private def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
    a.executorId.toInt < b.executorId.toInt
  }

}