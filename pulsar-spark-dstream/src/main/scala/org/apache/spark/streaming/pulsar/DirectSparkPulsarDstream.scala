package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.Schema
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.{StreamingContext, Time}

import java.{util => ju}
import scala.collection.convert.ImplicitConversions.`map AsScala`


private[spark] class DirectSparkPulsarDstream[T](_ssc: StreamingContext, //As both schema and streaming context are not serializable hence marking them transient, every field of this class must be serializable
                                  clientParams: ju.Map[String, AnyRef],
                                  readerParams: ju.Map[String, AnyRef],
                                  @transient schema: Schema[T],
                                  perPartitionConfig: PerPartitionConfig,
                                  storageLevel: StorageLevel
                                 ) extends InputDStream[SparkPulsarMessage[T]](_ssc) with Logging {

  private val enableRDDCheckpointing: Boolean = context.sparkContext.conf.get(PULSAR_RDD_CHECKPOINTING_ENABLED)
  private val autoAckEnabled: Boolean = context.sparkContext.conf.get(PULSAR_AUTO_ACK_ENABLED)
  private val initialRate = context.sparkContext.getConf.getLong(
    "spark.streaming.backpressure.initialRate", 0)

  if(enableRDDCheckpointing){
    require(autoAckEnabled, "Auto-ack is disabled in case of RDD checkpointing")
  }
  private val subscriptionName = PulsarProvider.getPredefinedSubscription(readerParams)
  private val topicName = PulsarProvider.getTopic(readerParams)
  private val schemaInfoSerializable = new SchemaInfoSerializable(schema.getSchemaInfo)

  //stores the partition->[start, end]
  private var offsetRangesPerPartition: ju.Map[String, PulsarOffsetRange] = new ju.HashMap[String, PulsarOffsetRange]

  persist(storageLevel) // set RDD storage level
  protected[streaming] override val mustCheckpoint = enableRDDCheckpointing

  override def start(): Unit = {
    PulsarHelper(clientParams).createSubscriptionIfNotAvailable(topicName, subscriptionName, PulsarProvider.getSubscriptionIntitialPosition(readerParams))
    context.addStreamingListener(new BatchListener(context.sc))
  }

  override def stop(): Unit = {
    PulsarHelper(clientParams).close()
  }

  def getPulsarPartitonOffsetRanges(validTime: Time): Array[PulsarPartition] = {
    val estimatedRateLimit = rateController.map { x => {
      val lr = x.getLatestRate()
      if (lr > 0) lr else initialRate
    }}
    offsetRangesPerPartition = PulsarHelper(clientParams).getOffsetRange(topicName,
      subscriptionName,
      PulsarProvider.getSubscriptionIntitialPosition(readerParams),
      offsetRangesPerPartition,
      perPartitionConfig,
      context.graph.batchDuration.milliseconds,
      estimatedRateLimit)

    logInfo(s"For time: ${validTime} --- \t" + offsetRangesPerPartition.map(partition => s"Topic name:+ ${partition._1}" +
      s"\tstarting Offset: ${partition._2.startMessageId}" +
      s"\tending offset: ${partition._2.endMessageId}" +
      s"\tbatch size: ${partition._2.batchSize}").toList.mkString("\n"))

    //Filter out empty batches
    offsetRangesPerPartition.filter(x => x._2.batchSize > 0).map(k => PulsarPartition(k._1, k._2, schemaInfoSerializable)).toArray
  }

  override def compute(validTime: Time): Option[RDD[SparkPulsarMessage[T]]] = {
    val pulsarPartitionArray = getPulsarPartitonOffsetRanges(validTime)

    val topicConsumerMetricsMap: Map[String, TopicConsumerMetrics] = pulsarPartitionArray.map(partition => partition.topic -> TopicConsumerMetrics.getMetricsForTopic(partition.topic, context.sc)).toMap

    val description = pulsarPartitionArray.map(x =>
      s"topic: ${x.topic} \t" +
        s"starting offset: ${x.pulsarOffsetRange.startMessageId}\t" +
        s"ending offset: ${x.pulsarOffsetRange.endMessageId}\t" +
        s"max count: ${x.pulsarOffsetRange.batchSize}").mkString("\n")

    logInfo(s"Creating RDD with mtd:\n $description")


    val rdd = new SparkPulsarRDD[T](context.sparkContext,
      clientParams,
      readerParams,
      pulsarPartitionArray, topicConsumerMetricsMap)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val metadata = Map(
      "offsets" -> pulsarPartitionArray,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    context.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    Some(rdd)
  }

  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(context.conf)) {
      Some(new DirectPulsarRateController(id,
        RateEstimator.create(context.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }
  private[streaming] class DirectPulsarRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = {

    }
  }

}




