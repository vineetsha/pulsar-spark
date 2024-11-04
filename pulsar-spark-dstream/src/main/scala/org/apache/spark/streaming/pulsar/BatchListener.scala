package org.apache.spark.streaming.pulsar

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped

private[pulsar] class BatchListener(sc: SparkContext) extends StreamingListener with Logging {
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    // Report all metrics to sinks
    logDebug("Updating and reporting metrics")
    TopicConsumerMetrics.updateMetrics()
    sc.env.metricsSystem.report()
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {}

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {}

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {}

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {}

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    logInfo("Reseting all consumer stats accumulators")
    TopicConsumerMetrics.resetAllAccumulators()
  }
}