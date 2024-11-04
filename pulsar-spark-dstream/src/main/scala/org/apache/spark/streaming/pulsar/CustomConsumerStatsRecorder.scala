package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.Message
import org.apache.pulsar.shade.io.netty.util
import org.apache.pulsar.shade.io.netty.util.TimerTask
import org.apache.spark.internal.Logging

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{DoubleAdder, LongAdder}
import java.{util => ju}

private[pulsar] class CustomConsumerStatsRecorder(clientConf: ju.Map[String, AnyRef],
                                  topicConsumerMetrics: TopicConsumerMetrics
                                 ) extends Logging {


  private var stat: TimerTask = null
  private var oldTime: Long = System.nanoTime
  private var statTimeout: util.Timeout = null
  private val statsIntervalSeconds: Long = 1L
  private val consumerStartTime = System.nanoTime()

  val numMsgsReceived: LongAdder = new LongAdder()
  val numBytesReceived: LongAdder = new LongAdder()


  val totalMsgsReceived: LongAdder = new LongAdder()
  val totalBytesReceived: LongAdder = new LongAdder()
  val totalReceivedFailed: LongAdder = new LongAdder()


  var receivedMsgsRate: Double = 0.0D
  var maxReceivedMsgsRate: Double = 0.0D
  var receivedByteRate: Double = 0.0D

  val cnt: DoubleAdder = new DoubleAdder()

  this.stat = new TimerTask {
    override def run(timeout: util.Timeout): Unit = {
      try {
        val now = System.nanoTime
        val elapsed = (now - oldTime).toDouble / 1.0E9D
        oldTime = now
        cnt.add(1L)
        val currentNumMsgsReceived = numMsgsReceived.sumThenReset
        val currentNumBytesReceived = numBytesReceived.sumThenReset
        val currentNumMsgsReceivedRate = currentNumMsgsReceived.toDouble / elapsed
        val currentNumBytesReceivedRate = currentNumBytesReceived.toDouble / elapsed

        receivedMsgsRate = receivedMsgsRate - (receivedMsgsRate - currentNumMsgsReceivedRate) / cnt.doubleValue()
        receivedByteRate = receivedByteRate - (receivedByteRate - currentNumBytesReceivedRate) / cnt.doubleValue()
        maxReceivedMsgsRate = math.max(maxReceivedMsgsRate, receivedMsgsRate)
      } catch {
        case var22: Exception => {
          logInfo("Error in ACC computation" + var22.getMessage)


        }
      } finally {
        statTimeout = CachedPulsarClient.getOrCreate(clientConf).timer.newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS)
      }
    }
  }
  statTimeout = CachedPulsarClient.getOrCreate(clientConf).timer.newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS)

  def updateNumMsgsReceived(message: Message[_]): Unit = {
    if (message != null) {
      totalMsgsReceived.add(1L)
      totalBytesReceived.add(message.size().toLong)
      this.numMsgsReceived.increment()
      this.numBytesReceived.add(message.size.toLong)
    }
  }

  def updateNumMsgsFailed(): Unit = {
    totalReceivedFailed.increment()
  }


  def lastRun() = {
    val now = System.nanoTime
    val elapsed = (now - oldTime).toDouble / 1.0E9D
    oldTime = now
    cnt.add(1L)
    val currentNumMsgsReceived = numMsgsReceived.sumThenReset
    val currentNumBytesReceived = numBytesReceived.sumThenReset
    val currentNumMsgsReceivedRate = currentNumMsgsReceived.toDouble / elapsed
    val currentNumBytesReceivedRate = currentNumBytesReceived.toDouble / elapsed

    receivedMsgsRate = receivedMsgsRate - (receivedMsgsRate - currentNumMsgsReceivedRate) / cnt.doubleValue()
    receivedByteRate = receivedByteRate - (receivedByteRate - currentNumBytesReceivedRate) / cnt.doubleValue()
    maxReceivedMsgsRate = math.max(maxReceivedMsgsRate, receivedMsgsRate)
  }

  def updateAccumulators(): Unit = {
    lastRun()
    val consumerStopTime = System.nanoTime()

    topicConsumerMetrics.updateAcc(ConsumerMetricNames.totalMsgsReceived, totalMsgsReceived.doubleValue())
    topicConsumerMetrics.updateAcc(ConsumerMetricNames.totalBytesReceived, totalBytesReceived.doubleValue())

    topicConsumerMetrics.updateAcc(ConsumerMetricNames.receivedMsgRate, receivedMsgsRate.doubleValue())
    topicConsumerMetrics.updateAcc(ConsumerMetricNames.receivedByteRate, receivedByteRate.doubleValue())
    topicConsumerMetrics.updateAcc(ConsumerMetricNames.maxReceiveRate, maxReceivedMsgsRate.doubleValue())

    topicConsumerMetrics.updateAcc(ConsumerMetricNames.consumerRuntime, (consumerStopTime - consumerStartTime).toDouble / 1.0E9D)
    topicConsumerMetrics.updateAcc(ConsumerMetricNames.totalReceiveFailed, totalReceivedFailed.doubleValue())
  }
  def updateConsumerCreationTime(start: Long): Unit = {
    topicConsumerMetrics.updateAcc(ConsumerMetricNames.consumerCreationTime, (System.nanoTime() - start).toDouble / 1.0E9D)
  }


}
