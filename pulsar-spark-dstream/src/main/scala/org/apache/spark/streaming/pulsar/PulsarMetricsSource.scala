package org.apache.spark.streaming.pulsar

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.SparkContext
import org.apache.spark.metrics.source.Source

private[spark] class PulsarMetricsSource extends Source {
  private val registry=new MetricRegistry

  def registerGauge(sc: SparkContext, consumerMetrics: TopicConsumerMetrics): Unit = {
    consumerMetrics.getMetricMap().values.foreach(consumerMetric => {

      sc.register(consumerMetric.getAccumulator(), consumerMetric.name()) //Register accumulator
      metricRegistry.register(MetricRegistry.name(consumerMetric.name()), new Gauge[Double] {
        override def getValue: Double = consumerMetric.value()
      })
    }
    )
  }
  override def sourceName: String = "PulsarMetricSource"
  override def metricRegistry: MetricRegistry = registry
}

object PulsarMetricsSource {
  def register(sc: SparkContext, topicConsumerMetric: TopicConsumerMetrics): Unit = {
    val source = new PulsarMetricsSource
    source.registerGauge(sc, topicConsumerMetric)
    sc.env.metricsSystem.registerSource(source)
  }
}
