package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.{MessageId, SubscriptionInitialPosition}
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.pulsar.PulsarLedger.{getEndMessageId, getNextMessageId}
import org.apache.spark.streaming.pulsar.PulsarContants.LEDGER_ENTRY_SEPARATOR
import org.apache.spark.streaming.pulsar.PulsarProvider.getDefaultMsgId

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.{util => ju}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.mapAsJavaMapConverter

class PulsarHelper(clientConf: ju.Map[String, AnyRef]) extends Serializable with Logging with AutoCloseable {
  private val ZERO_BACKLOG=0L

  override def close(): Unit = {
    CachedPulsarAdmin.clearActivePulsarAdmin()
  }

  def resetCursor(topic: String, fQsubscription: String, msgId: MessageId): Unit = {
    logInfo(s" Resetting Partition: ${topic} -- subscription: ${fQsubscription} cursor to ${msgId}")
    CachedPulsarAdmin.getOrCreate(clientConf).resetCursor(topic, fQsubscription, msgId)
    logInfo(s"Partition: ${topic} -- subscription: ${fQsubscription} reset to ${msgId}")
  }

  def createSubscriptionIfNotAvailable(topicName: String, subscriptionName: String, subscriptionInitialPosition: SubscriptionInitialPosition) = {
    CachedPulsarAdmin.getOrCreate(clientConf).createSubscriptionIfNotAvailable(topicName, subscriptionName, getDefaultMsgId(subscriptionInitialPosition))
  }

  def getMarkDeletePosititon(stats: PersistentTopicInternalStats,
                             topicPartition: String,
                             subscriptionInitialPosition: SubscriptionInitialPosition,
                             fullyQualifiedSubscriptionName: String
                            ): MessageIdImpl = {
    require(stats!=null, s"${topicPartition} stats are not available")

    val subscriptionName = encodeValue(fullyQualifiedSubscriptionName)
    val cursorStats = stats.cursors.get(subscriptionName)
    //first batch on start.
    //could be due to restart or first every consumption batch.

    //directly read from pulsar cursor
    if (cursorStats != null) { // To check whether subscription is present or not.
      logInfo(s"Topic partition: ${topicPartition} \n" +
        "\t\t\t\t\t mark delete position: " + cursorStats.markDeletePosition + "\n" +
        "\t\t\t\t\t read position: " + cursorStats.readPosition + "\n" +
        "\t\t\t\t\t pending read ops: " + cursorStats.pendingReadOps + "\n" +
        "\t\t\t\t\t messagesConsumedCounter: " + cursorStats.messagesConsumedCounter + "\n" +
        "\t\t\t\t\t subscriptionHavePendingRead: " + cursorStats.subscriptionHavePendingRead
      )
      val offset = cursorStats.markDeletePosition.split(LEDGER_ENTRY_SEPARATOR)
      val ledger = offset(0).toLong
      val entry = offset(1).toLong // could be -1, in case of starting of ledger
      val partition = TopicName.get(topicPartition).getPartitionIndex
      new MessageIdImpl(ledger, entry, partition)
    }
    else {
      logWarning(s"Cursor stats not available for partition: ${topicPartition}")
      getDefaultMsgId(subscriptionInitialPosition).asInstanceOf[MessageIdImpl]
    }

  }


  def defaultEntriesToFetchPerPartition(ppc: PerPartitionConfig, tp: TopicPartition, batchInterval: Long): Long = {
    ((ppc.maxRatePerPartition(tp) * batchInterval / 1000) / ppc.avgMsgsPerEntry(tp)).toLong // Round it out to ceil
  }

  def computeMaxEntriesToReadPerPartition(estimatedRateLimit: Option[Long], ppm: ju.Map[String, PulsarPartitionMetadata], batchInterval: Long, ppc: PerPartitionConfig) = {
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        log.info(s"Spark estimated rate limit: ${rate}")

        val totalLag = ppm.map { case (tp, mtd) => mtd.backlog }.sum

        ppm.map { case (tp, mtd) =>
          val topicPartition = new TopicPartition(TopicPartition.getTopicName(tp), partition = TopicName.getPartitionIndex(tp))
          val maxRateLimitPerPartition = ppc.maxRatePerPartition(topicPartition) / ppc.avgMsgsPerEntry(topicPartition)
          val backpressureRate = mtd.backlog / totalLag.toDouble * rate
          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.max(Math.min(backpressureRate, maxRateLimitPerPartition), ppc.minRatePerPartition(topicPartition))
          } else backpressureRate)
        }
      case None => {
        log.info("Spark estimated rate not available")
        ppm.map { case (tp, mtd) =>
          val topicPartition = new TopicPartition(TopicPartition.getTopicName(tp), partition = TopicName.getPartitionIndex(tp))
          tp -> ppc.maxRatePerPartition(topicPartition).toDouble / ppc.avgMsgsPerEntry(topicPartition)
        }
      }
    }

    val secsPerBatch = batchInterval.toDouble / 1000

    effectiveRateLimitPerPartition.map {
      case (tp, limit) =>
        val mtd=ppm.get(tp)
        if(mtd!=null){
          tp -> Math.min((secsPerBatch * limit).ceil.toLong, mtd.backlog)
        }
        else{
          tp -> ZERO_BACKLOG
        }
    }
  }


  def computeMetaDataForEachPartition(topicName: String, fQSubscription: String, subscriptionInitialPosition: SubscriptionInitialPosition): ju.Map[String, PulsarPartitionMetadata] = {
    val stats = CachedPulsarAdmin.getOrCreate(clientConf).getPartitionedInternalStats(topicName)
    stats.partitions.map {
      case (topicPartition, persistentTopicInternalStats) =>
        val markDeletePosition = getMarkDeletePosititon(persistentTopicInternalStats, topicPartition, subscriptionInitialPosition, fQSubscription)
        val ledgers = PulsarLedger.createLedgers(persistentTopicInternalStats)
        val backlog = PulsarLedger.computeBacklog(ledgers, Some(markDeletePosition))
        topicPartition -> PulsarPartitionMetadata(ledgers, backlog, markDeletePosition)
    }

  }

  def getOffsetRange(topicName: String,
                     fQSubscriptionName: String,
                     subscriptionInitialPosition: SubscriptionInitialPosition,
                     offsetRangeMap: ju.Map[String, PulsarOffsetRange],
                     ppc: PerPartitionConfig,
                     batchInterval: Long,
                     estimatedRateLimit: Option[Long]): ju.Map[String, PulsarOffsetRange] = {

    val topicPartitionMetadata = computeMetaDataForEachPartition(topicName, fQSubscriptionName, subscriptionInitialPosition)

    val maxEntriesToReadPerPartition = computeMaxEntriesToReadPerPartition(estimatedRateLimit, topicPartitionMetadata, batchInterval, ppc)


    topicPartitionMetadata.map { case (partition, ppm) => {
      val ledgers = ppm.ledgers
      val topicPartition = new TopicPartition(topicName, partition = TopicName.getPartitionIndex(partition))
      val batchSize = maxEntriesToReadPerPartition.getOrDefault(partition, defaultEntriesToFetchPerPartition(ppc, topicPartition, batchInterval)) // Round it out to ceil
      logInfo(s"${partition}: Max entries to fetch ${batchSize}")


      if (offsetRangeMap.contains(partition)) {

        //nextMessageId(markDeletePosition)
        val lastReadPosition = offsetRangeMap.get(partition).endMessageId

        val nextStartingOffset = getNextMessageId(ledgers, lastReadPosition)
        if (nextStartingOffset.isDefined) {
          partition -> computeOffsetRange(ledgers, nextStartingOffset.get, batchSize.toInt, TopicName.getPartitionIndex(partition))
        } else {
          //next batch cannot be defined hence 0 as batch size
          partition -> PulsarOffsetRange(offsetRangeMap.get(partition).endMessageId, offsetRangeMap.get(partition).endMessageId, batchSize = 0)
        }
      }
      else { // read from pulsar, pulsar startMessageId could be stale the implementation will take care of that also
        // calculating next messageId because this value is fetched from markDeletePosition, which is last acked message
        val nextStartingOffset = getNextMessageId(ledgers, ppm.markDeletePosition)
        if (nextStartingOffset.isDefined) {
          partition -> computeOffsetRange(ledgers, nextStartingOffset.get, batchSize.toInt, TopicName.getPartitionIndex(partition))
        } else {
          //next batch cannot be defined hence 0 as batch size
          partition -> PulsarOffsetRange(ppm.markDeletePosition, ppm.markDeletePosition, batchSize = 0)
        }
      }
    }
    }.toMap[String, PulsarOffsetRange].asJava
  }


  //this is called when we know there are messages available from startMessageId
  def computeOffsetRange(ledgers: Seq[PulsarLedger], startMessageId: MessageIdImpl, batchSize: Int, partition: Int): PulsarOffsetRange = {
    if (ledgers.isEmpty) {
      PulsarOffsetRange(startMessageId, startMessageId, 0)
    }
    val value = getEndMessageId(ledgers, startMessageId, batchSize, partition)
    PulsarOffsetRange(startMessageId = startMessageId, endMessageId = value._1, value._2.toInt)
  }

  private def encodeValue(value: String) = URLEncoder.encode(value, StandardCharsets.UTF_8.toString)
}

object PulsarHelper {
  private var pulsarHelper: PulsarHelper = null

  def apply(clientConf: ju.Map[String, AnyRef]): PulsarHelper = {
    if (pulsarHelper == null) {
      pulsarHelper = new PulsarHelper(clientConf)
    }
    pulsarHelper
  }
}
