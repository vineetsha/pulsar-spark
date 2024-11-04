package org.apache.spark.streaming.pulsar


import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.pulsar.ConsumerMetricNames._
import org.apache.spark.util.{DoubleAccumulator}

import scala.collection.mutable

trait ConsumerMetric extends Serializable with Logging{
  def name(): String
  def value(): Double
  def getAccumulator(): DoubleAccumulator
  def resetAccumulator(): Unit
  def updateMetricValue(): Unit
  def setAccumulator(value: Double)= getAccumulator().setValue(value)

}
abstract class AbstractConsumerMetric(topicName: String, metricName: String) extends ConsumerMetric{
  val acc=new DoubleAccumulator
  private var previousValue: Double=0
  def name=topicName+"."+metricName

  override def value(): Double= {
    previousValue
  }
  def updateMetricValue(): Unit ={
    this.previousValue=acc.value
  }
  def resetAccumulator():Unit={
    acc.reset()
  }
  override def getAccumulator(): DoubleAccumulator = acc
}

private[spark] case class ConsumerRunTime(topicName:String) extends AbstractConsumerMetric(topicName, consumerRuntime){}
private[spark] case class ReceivedBytesRate(topicName:String) extends AbstractConsumerMetric(topicName, receivedByteRate){}
private[spark] case class ReceivedMsgsRate(topicName:String) extends AbstractConsumerMetric(topicName, receivedMsgRate){}
private[spark] case class TotalBytesReceived(topicName:String) extends AbstractConsumerMetric(topicName, totalBytesReceived){}
private[spark] case class TotalMsgsReceived(topicName:String) extends AbstractConsumerMetric(topicName, totalMsgsReceived){}
private[spark] case class TotalReceiveFailed(topicName:String) extends AbstractConsumerMetric(topicName, totalReceiveFailed){}
private[spark] case class MaxReceiveRate(topicName:String) extends AbstractConsumerMetric(topicName, maxReceiveRate){}
private[spark] case class ConsumerCreationTime(topicName: String) extends AbstractConsumerMetric(topicName, consumerCreationTime){}
private[spark] case class ResetCursorLatency(topicName: String) extends AbstractConsumerMetric(topicName, resetCursorLatency){}


private[spark] class TopicConsumerMetrics(topicName: String) extends Serializable {

  private final val metricMap:Map[String, ConsumerMetric]=Map(
    consumerRuntime-> ConsumerRunTime(topicName),
    receivedByteRate-> ReceivedBytesRate(topicName),
    receivedMsgRate-> ReceivedMsgsRate(topicName),
    totalBytesReceived -> TotalBytesReceived(topicName),
    totalMsgsReceived-> TotalMsgsReceived(topicName),
    totalReceiveFailed-> TotalReceiveFailed(topicName),
    maxReceiveRate-> MaxReceiveRate(topicName),
    consumerCreationTime-> ConsumerCreationTime(topicName),
    resetCursorLatency-> ResetCursorLatency(topicName)
  )
  def getMetricMap()=metricMap

  def resetAllAcc():Unit={
    metricMap.foreach(x=> x._2.resetAccumulator())
  }
  private def getAccumulator(metricName: String): ConsumerMetric ={
    metricMap.getOrElse(metricName, throw new IllegalArgumentException(s"${metricName} accumulator do not exist for topic: ${topicName}"))
  }
  def updateAcc(metricName: String, value: Double): Unit={
    getAccumulator(metricName).setAccumulator(value)
  }
  override def toString(): String={
    topicName+"---> \n"+ metricMap.map(pair=> {
      pair._2.name() +" : "+ pair._2.value()
    }).mkString(" \n ")
  }

}
object TopicConsumerMetrics{
  val topicConsumerMetricsMap: mutable.Map[String, TopicConsumerMetrics]=mutable.Map()

  def getMetricsForTopic(topicName: String,sc: SparkContext): TopicConsumerMetrics ={
    topicConsumerMetricsMap.getOrElseUpdate(topicName, {
      val topicConsumerMetrics=new TopicConsumerMetrics(topicName)
      PulsarMetricsSource.register(sc, topicConsumerMetrics)
      topicConsumerMetrics
    })

  }
  def updateMetrics():Unit={
    topicConsumerMetricsMap.values.foreach(topicConsumerMetrics=> topicConsumerMetrics.metricMap.values.foreach(consumerMetric=> consumerMetric.updateMetricValue()))
  }
  def resetAllAccumulators():Unit={
    topicConsumerMetricsMap.foreach(pair=> pair._2.resetAllAcc())
  }

  def getAllAccumulators(): String={
    topicConsumerMetricsMap.map(pair=> pair._2.toString()).mkString("\n\n")
  }
}


object ConsumerMetricNames{
  final val receivedByteRate = "receivedBytesRate"
  final val receivedMsgRate = "receivedMsgsRate"
  final val totalBytesReceived = "totalBytesReceived"
  final val totalMsgsReceived = "totalMsgsReceived"
  final val totalReceiveFailed = "totalReceiveFailed"
  final val consumerRuntime="consumerRuntime"
  final val maxReceiveRate= "maxReceiveRate"
  final val consumerCreationTime="consumerCreationTime"
  final val resetCursorLatency="resetCursorLatency"
}

