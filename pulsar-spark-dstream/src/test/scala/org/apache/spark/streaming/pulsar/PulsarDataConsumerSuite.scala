package org.apache.spark.streaming.pulsar

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pulsar.client.api.{Message, MessageId, Schema, SubscriptionInitialPosition}
import org.apache.pulsar.client.api.url.PulsarURLStreamHandlerFactory
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.spark.SparkFunSuite
import PulsarClientUtils.{PULSAR_CLIENT_PARAMS, sendMessages}
import org.junit.Assert.assertEquals

import java.net.{MalformedURLException, URI, URISyntaxException, URL}
import java.util.Base64
import java.{util => ju}
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ListBuffer

class PulsarDataConsumerSuite extends SparkFunSuite {

  test("pulsar bounded consumer"){
    val topicName="persistent://cl-optimize/pwc-test/test-2"
    val sub="cl-optimize/reader-sub-7"

    val listBuff: ListBuffer[Array[Byte]] = ListBuffer()
    for (i <- 1 to 100) {
      listBuff.append(s"ID-${i} name-${i}".getBytes)
    }
    val mids=PulsarClientUtils.sendMessages[Array[Byte]](PulsarClientUtils.getViestiClient, Schema.BYTES, topicName, listBuff.toList.asJava)

    val firstMid=mids.get(0)
    val lastMid: MessageId=mids.get(mids.size()-1)

    val params=PulsarClientUtils.getClientAndReaderParams(PULSAR_CLIENT_PARAMS, topicName, sub, SubscriptionInitialPosition.Earliest);
    val clientParams=params.getKey
    val readerParams=params.getValue

    val pulsarPartition=PulsarPartition("persistent://cl-optimize/pwc-test/test-2-partition-0",
      PulsarOffsetRange(firstMid.asInstanceOf[MessageIdImpl],lastMid.asInstanceOf[MessageIdImpl], 100),
      Schema.BYTES.getSchemaInfo)
    val topicConsumerMetrics=new TopicConsumerMetrics("persistent://cl-optimize/pwc-test/test-2-partition-0")
    val boundedConsumer=new PulsarBoundedConsumer(pulsarPartition , clientParams, readerParams, topicConsumerMetrics, true)

    val it=boundedConsumer.internalIterator
    var lastMsgIdReceived: MessageId=null

    while(it.hasNext){
      val lastMsgReceived=it.next()
      lastMsgIdReceived = lastMsgReceived.getMessageId
      SparkPulsarMessage(lastMsgReceived.asInstanceOf[Message[Array[Byte]]]) //Check for conversion
    }
    boundedConsumer.close()
    assertEquals(lastMid, lastMsgIdReceived)


  }


}
