package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.{MessageId, Schema}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

import java.{util => ju}


object PulsarStreamUtil extends Logging {


  //return spark pulsar rdd for batch use case
  def createRDD[T](jsc: JavaSparkContext,
                   pulsarParams: ju.Map[String, AnyRef],
                   pulsarPartition: Array[PulsarPartition]): JavaRDD[SparkPulsarMessage[T]] ={
    new JavaRDD(PulsarStreamUtil.createRDD[T](jsc.sc, pulsarParams, pulsarPartition))
  }
  def createRDD[T](sc: SparkContext,
                   pulsarParams: ju.Map[String, AnyRef],
                   pulsarPartition: Array[PulsarPartition]): RDD[SparkPulsarMessage[T]] = {

    val clientParams = PulsarProvider.getClientParams(pulsarParams)
    val readerParams = PulsarProvider.getReaderParams(pulsarParams)

    logInfo(s"Pulsar client Params: ${clientParams}")

    PulsarProvider.validateReaderOptions(readerParams)

    PulsarProvider.validateClientParams(clientParams)
    new SparkPulsarRDD[T](sc,
      clientParams,
      readerParams,
      pulsarPartition, pulsarPartition.map(pulsarPartition => pulsarPartition.topic ->TopicConsumerMetrics.getMetricsForTopic(pulsarPartition.topic, sc))
        .toMap[String, TopicConsumerMetrics])
  }


  def createDirectStream(ssc: StreamingContext,
                         pulsarParams: ju.Map[String, AnyRef]
                        ): InputDStream[SparkPulsarMessage[Array[Byte]]] =
    createDirectStream[Array[Byte]](
      ssc,
      pulsarParams,
      Schema.BYTES
    )

  def createDirectStream[T](jssc: JavaStreamingContext,
                            pulsarParams: ju.Map[String, AnyRef],
                            schema: Schema[T]
                           ): JavaInputDStream[SparkPulsarMessage[T]] =
    new JavaInputDStream(createDirectStream[T](
      jssc.ssc,
      pulsarParams,
      schema,
      StorageLevel.MEMORY_ONLY)
    )

  def createDirectStream[T](ssc: StreamingContext,
                            pulsarParams: ju.Map[String, AnyRef],
                            schema: Schema[T]
                           ): InputDStream[SparkPulsarMessage[T]] =
    createDirectStream[T](
      ssc,
      pulsarParams,
      schema,
      StorageLevel.MEMORY_ONLY
    )

  def createDirectStream[T](ssc: StreamingContext,
                            pulsarParams: ju.Map[String, AnyRef],
                            schema: Schema[T],
                            storageLevel: StorageLevel
                           ): InputDStream[SparkPulsarMessage[T]] =
    createDirectStream[T](
      ssc,
      pulsarParams,
      schema,
      storageLevel,
      new DefaultPerPartitionConfig(ssc.conf)
    )



  def createDirectStream[T](ssc: StreamingContext,
                            pulsarParams: ju.Map[String, AnyRef],
                            schema: Schema[T],
                            storageLevel: StorageLevel,
                            ppc: PerPartitionConfig
                           ): InputDStream[SparkPulsarMessage[T]] = {

    val clientParams = PulsarProvider.getClientParams(pulsarParams)
    val readerParams = PulsarProvider.getReaderParams(pulsarParams)

    PulsarProvider.validateReaderOptions(readerParams)

    PulsarProvider.validateClientParams(clientParams)
    new DirectSparkPulsarDstream[T](ssc,
      clientParams,
      readerParams,
      schema,
      ppc,
      storageLevel
    )
  }
}

object OffsetCommitUtil extends Serializable with Logging {

  def commitOffset[T](rdd: RDD[SparkPulsarMessage[T]], pulsarParams: ju.Map[String, AnyRef]): Unit = {
    logDebug(s"Calling reset cursor for RDD.")
    val clientParams = PulsarProvider.getClientParams(pulsarParams)
    val readerParams = PulsarProvider.getReaderParams(pulsarParams)

    PulsarProvider.validateReaderOptions(readerParams)

    PulsarProvider.validateClientParams(clientParams)


    val map = rdd.asInstanceOf[SparkPulsarRDD[T]].consumerStatsMetricMap
    rdd.foreachPartition(sparkPulsarPartition => {
      val msgs = sparkPulsarPartition.toList
      if (msgs.size > 0) {
        val lastMessage = msgs.last
        val topic = lastMessage.topicName
        val msgId = lastMessage.messageId
        val startTime = System.nanoTime()
        logDebug(s"Calling reset cursor for partition: ${topic}")
        PulsarHelper(clientParams).resetCursor(topic, readerParams.get(PulsarContants.PredefinedSubscription).toString, msgId)
        logDebug(s"Cursor resetted for partition: ${topic}")

        if(map.contains(topic)) {
          logDebug(s"Updating metrics: ${topic}")
          map.get(topic).get.updateAcc(ConsumerMetricNames.resetCursorLatency, (System.nanoTime() - startTime).toDouble / 1.0E9D)
        }
      }
    }
    )
  }

  def commitOffset(topicName: String, messageId: MessageId, pulsarParams: ju.Map[String, AnyRef]): Unit = {
    val clientParams = PulsarProvider.getClientParams(pulsarParams)
    val readerParams = PulsarProvider.getReaderParams(pulsarParams)
    PulsarProvider.validateReaderOptions(readerParams)
    PulsarProvider.validateClientParams(clientParams)
    val subscription = readerParams.get(PulsarContants.PredefinedSubscription).toString
    PulsarHelper(clientParams).resetCursor(topicName, subscription, messageId)
  }
}

