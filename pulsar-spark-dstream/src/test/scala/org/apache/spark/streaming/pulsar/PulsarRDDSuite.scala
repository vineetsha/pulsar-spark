package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.{MessageId, Schema, SubscriptionInitialPosition}
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.junit.Assert.assertEquals

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ListBuffer

class PulsarRDDSuite extends SparkFunSuite{
  private val sparkConf = new SparkConf().setAppName("spark App")
    .setMaster("spark://localhost:7077")
    .set("spark.logConf", "true")
    .set("spark.streaming.pulsar.autoAck.enabled", "true")
  private var sc: SparkContext=null

  override def beforeAll(): Unit={
    super.beforeAll()
    if(sc==null) {
      sc = new SparkContext(sparkConf)
    }
    else if(sc.isStopped){
      log.info("Spark context is stopped")
    }

  }

  override def afterAll(): Unit = {
    if(sc!=null){
      sc.stop()
    }
    super.afterAll()
  }

  test("Test spark pulsar RDD"){
    val topicName="persistent://cl-optimize/pwc-test/test-3"
    val sub="cl-optimize/pwc-sub"
    val listBuff: ListBuffer[Array[Byte]] = ListBuffer()
    for (i <- 1 to 1000) {
      listBuff.append(s"ID-${i} name-${i}".getBytes)
    }
    val mids=PulsarClientUtils.sendMessages[Array[Byte]](PulsarClientUtils.getViestiClient, Schema.BYTES, topicName, listBuff.toList.asJava)
    val firstMid: MessageId=mids.get(0)
    val lastMid: MessageId=mids.get(mids.size()-1)

    val pulsarOffsetRange=PulsarOffsetRange(firstMid.asInstanceOf[MessageIdImpl], lastMid.asInstanceOf[MessageIdImpl], mids.size())

    val pulsarPartition: Array[PulsarPartition]=Array(PulsarPartition(topicName+"-partition-0",pulsarOffsetRange,Schema.BYTES.getSchemaInfo))
    val pulsarParams= PulsarClientUtils.getPulsarParams(PulsarClientUtils.PULSAR_CLIENT_PARAMS, topicName, sub, SubscriptionInitialPosition.Earliest)
    val rdd=PulsarStreamUtil.createRDD(sc, pulsarParams, pulsarPartition)
    rdd.persist()
    val msgPayload= rdd.map(m=> new String(m.data)).collect().mkString("\n")
    val messages=rdd.collect()

    logInfo(s"Messages Recevied: $msgPayload")
    assertEquals(lastMid, messages.last.messageId)


  }
}
