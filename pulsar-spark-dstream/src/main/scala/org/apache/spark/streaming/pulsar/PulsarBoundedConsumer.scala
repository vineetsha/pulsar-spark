package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.{Consumer, Message, Schema, SubscriptionType}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

import java.io.Closeable
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.{util => ju}

private[pulsar] class PulsarBoundedConsumer[T](pulsarPartition: PulsarPartition,
                               clientConf: ju.Map[String, AnyRef],
                               readerConf: ju.Map[String, AnyRef],
                               topicConsumerMetrics: TopicConsumerMetrics,
                               autoAckEnabled: Boolean
                              ) extends Closeable with TaskCompletionListener with TaskFailureListener with Logging {

  val schema: Schema[_]=Schema.getSchema(pulsarPartition.siSerializable.si)


  val customConsumerStatsRecorder: CustomConsumerStatsRecorder = new CustomConsumerStatsRecorder(clientConf, topicConsumerMetrics)

  val subscription = PulsarProvider.getPredefinedSubscription(readerConf)

  private val internalBatchConsumer: Consumer[T] = getConsumer()


  def close() = {
    internalBatchConsumer.close()
  }

  @transient private var lastMessageId: MessageIdImpl = null

  lazy val internalIterator: Iterator[Message[T]] = new Iterator[Message[T]] {
    var startTime: Double = 0D

    override def hasNext: Boolean = {
      if (!compare(lastMessageId, pulsarPartition.pulsarOffsetRange.endMessageId)) {
        logInfo(s"Last MessageId Recevied: ${lastMessageId} endMessage Offset: ${pulsarPartition.pulsarOffsetRange.endMessageId}")
      }
      compare(lastMessageId, pulsarPartition.pulsarOffsetRange.endMessageId)
    }

    override def next(): Message[T] = {
      if (lastMessageId == null) {
        startTime = System.nanoTime()
      }
      val msg: Message[T] = internalBatchConsumer.receive(PulsarProvider.pollTimeoutMs, TimeUnit.MILLISECONDS)
      if (msg == null) {
        customConsumerStatsRecorder.updateNumMsgsFailed()
        throw new IllegalStateException(s"Failed to fetch message within poll out time for topic: ${getTopic()}")
      }
      else {
        customConsumerStatsRecorder.updateNumMsgsReceived(msg)
      }
      if (lastMessageId == null) {
        logDebug(s"first MessageId: ${msg.getMessageId.asInstanceOf[MessageIdImpl]}")
      }
      lastMessageId = msg.getMessageId.asInstanceOf[MessageIdImpl]
      msg
    }
  }

  def compareMid(mid1: MessageIdImpl, mid2: MessageIdImpl): Boolean = {
    (mid1, mid2) match {
      case (bm1: BatchMessageIdImpl, m2: MessageIdImpl) => {
        if (bm1.getEntryId < m2.getEntryId) {
          true
        }
        else if (bm1.getEntryId == m2.getEntryId) {
          if (bm1.getBatchIndex + 1 == bm1.getBatchSize) {
            return false
          }
          true
        }
        else {
          false
        }
      }
      case (m1: MessageIdImpl, m2: MessageIdImpl) =>
        m1.getEntryId < m2.getEntryId
      case _ => throw new IllegalStateException("invalid msgId")

    }
  }

  private def compare(mid1: MessageIdImpl, mid2: MessageIdImpl): Boolean = {
    if (mid1 == null) {
      return true
    }
    if (mid2 == null) {
      return false
    }
    if (mid1.getLedgerId < mid2.getLedgerId) {
      true
    }
    else if (mid1.getLedgerId == mid2.getLedgerId) {
      compareMid(mid1, mid2)
    }
    else {
      false
    }
  }


  override def onTaskCompletion(context: TaskContext): Unit = {
    if (context.isCompleted()) {
      // Mark acknowledgement only in case of task completition.
      if (lastMessageId != null && autoAckEnabled) {
        internalBatchConsumer.acknowledgeCumulative(lastMessageId)
      }
      logDebug(s"Closing Consumer ${internalBatchConsumer.getConsumerName}")
      close()
      customConsumerStatsRecorder.updateAccumulators()
    }
  }

  def getConsumer(): Consumer[T] = {
    logDebug(s"Creating consumer for partition: ${getTopic()} Range[ Starting offset: " + pulsarPartition.pulsarOffsetRange.startMessageId + " ending Offset:" + pulsarPartition.pulsarOffsetRange.endMessageId + "]")
    val start = System.nanoTime()
    logInfo(s"schema: ${schema.getSchemaInfo.getName}")
    val consumer = CachedPulsarClient.getOrCreate(clientConf).newConsumer(schema)
      .topic(getTopic())
      .consumerName(UUID.randomUUID.toString)
      .subscriptionInitialPosition(PulsarProvider.getSubscriptionIntitialPosition(readerConf))
      .subscriptionName(subscription)
      .subscriptionType(SubscriptionType.Exclusive)
      .subscribe().asInstanceOf[Consumer[T]]
    customConsumerStatsRecorder.updateConsumerCreationTime(start)
    consumer
  }

  def getTopic(): String = {
    pulsarPartition.topic
  }

  override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
    log.error(s"Task attempt: ${context.taskAttemptId()} failed.")
    log.error(s"Error: ${error.getMessage}")
    if (this.internalBatchConsumer != null) {
      logDebug(s"Closing Consumer ${internalBatchConsumer.getConsumerName}")
      this.internalBatchConsumer.close()
    }

  }
}

