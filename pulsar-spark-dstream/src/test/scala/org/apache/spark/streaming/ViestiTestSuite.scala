package org.apache.spark.streaming

import org.apache.pulsar.client.api.{MessageId, PulsarClient, Schema, SubscriptionInitialPosition}
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.pulsar._
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.concurrent.Eventually
import PulsarClientUtils.PULSAR_CLIENT_PARAMS

import java.util.concurrent.atomic.AtomicLong
import java.{util => ju}
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ListBuffer

class ViestiTestSuite extends SparkFunSuite
  with LocalStreamingContext
  with Eventually
  with Logging {

  val lastEntry=new AtomicLong(-1)
  val sparkConf = new SparkConf().setAppName("spark App")
    .setMaster("spark://localhost:7077")
    .set("spark.executor.instances", "4")
    .set("spark.streaming.pulsar.maxRatePerPartition", "1")
    .set("spark.streaming.pulsar.avgMsgPerEntry", "1.0")
    .set("spark.streaming.pulsar.rddCheckpointing.enabled", "false")
    .set("spark.streaming.pulsar.autoAck.enabled", "false")
    .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:src/test/resources/log4j.properties -Dcom.sun.management.jmxremote.port=1250 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true -Dsun.io.serialization.extendedDebugInfo=true -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 """)
    .set("spark.logConf", "true")
    .set("spark.driver.extraJavaOptions", """-Dlog4j.configuration=file:src/test/resources/log4j.properties -Dcom.sun.management.jmxremote.port=1251 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true -Dsun.io.serialization.extendedDebugInfo=true""")

  lazy val checkpointDir = "src/test/resources/checkpointDir"

  lazy val streamingInterval = Durations.seconds(20)

  def functionToCreateContext(): StreamingContext = {
    val ssc = new StreamingContext(sparkConf, streamingInterval) // new context
    ssc.checkpoint(checkpointDir)
    ssc
  }



  test("Test checkpointing") {
    sparkConf.set("spark.streaming.pulsar.rddCheckpointing.enabled", "true")
      .set("spark.streaming.pulsar.autoAck.enabled", "true")
    val topicName = "persistent://cl-optimize/pwc-test/test-1"
    val subscription = "cl-optimize/reader-sub"
    val pulsarConf = getPulsarConf(topicName, subscription)

    val listBuff: ListBuffer[Array[Byte]] = ListBuffer()
    for (i <- 1 to 100) {
      listBuff.append(s"ID-${i} name-${i}".getBytes)
    }
    PulsarClientUtils.sendMessages[Array[Byte]](PulsarClient.builder()
      .loadConf(PULSAR_CLIENT_PARAMS)
      .build(), Schema.BYTES, topicName, listBuff.toList.asJava)


    val ssc = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext)
    var recoveredStreamingContext: StreamingContext=null


    try {

      val stream: InputDStream[SparkPulsarMessage[Array[Byte]]] = PulsarStreamUtil.createDirectStream[Array[Byte]](ssc, pulsarConf, Schema.BYTES, StorageLevel.MEMORY_AND_DISK_SER)
      val keyedStream = stream.map { r => "key" -> r.messageId.asInstanceOf[MessageIdImpl].getEntryId }
      val stateStream = keyedStream.updateStateByKey { (values: Seq[Long], state: Option[Long]) =>
        Some(values.sum + {
          if(state.isEmpty) 0 else state.get
        })
      }


      // This is ensure all the data is eventually receiving only once
      stateStream.foreachRDD { (rdd: RDD[(String, Long)]) =>
        rdd.checkpoint()
        rdd.collect().headOption.foreach { x =>
          println(x)
        }
      }


      ssc.start()

      ssc.awaitTerminationOrTimeout(60000)

      ssc.stop(true, true)



      recoveredStreamingContext=StreamingContext.getOrCreate(checkpointDir, functionToCreateContext)
      recoveredStreamingContext.start()

      recoveredStreamingContext.awaitTerminationOrTimeout(60000)
      recoveredStreamingContext.stop(stopSparkContext = true, stopGracefully = true)

    }
    catch {
      case e: Exception =>
        logError("Exceptions in executing Pulsar-spark App: " + e.getMessage, e)
        throw e
    }
    finally {
      logInfo("stopped")
    }
  }



  def getPulsarConf(topicName: String, sub: String): ju.Map[String, AnyRef] = {
    val pulsarConf = new ju.HashMap[String, AnyRef]()
    pulsarConf.put("pulsar.client.serviceUrl", PULSAR_CLIENT_PARAMS.get(PulsarContants.ServiceUrlOptionKey))
    pulsarConf.put("pulsar.client.authPluginClassName", PULSAR_CLIENT_PARAMS.get(PulsarContants.AuthPluginClassName))
    pulsarConf.put("pulsar.client.authParams", PULSAR_CLIENT_PARAMS.get(PulsarContants.AuthParams))
    pulsarConf.put("pulsar.reader.subscriptionName", sub)
    pulsarConf.put("pulsar.reader.topicNames", topicName)
    pulsarConf.put("pulsar.reader.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
    pulsarConf
  }

  def ser(a: AnyRef) =
    (new java.io.ObjectOutputStream(new java.io.ByteArrayOutputStream())).writeObject(a)

  test("PPC serialization check") {
    ser(new DefaultPerPartitionConfig(sparkConf))
  }

}
