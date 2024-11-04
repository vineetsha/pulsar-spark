package org.apache.spark.streaming

import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.spark.streaming.pulsar._
import org.apache.spark.{SparkConf, SparkFunSuite}

import java.{util => ju}
class UnitTests extends SparkFunSuite {
  test("getEndMessageIdTest") {
    val pulsarHelper=PulsarHelper(null)
    logInfo("created pulsar helper")

    val pulsarLedgers=Seq(PulsarLedger(12, 20),
      PulsarLedger(15, 19),
        PulsarLedger(16, 30),
      PulsarLedger(19, 28),
      PulsarLedger(20, 18))
    val endMessageId=PulsarLedger.getEndMessageId(pulsarLedgers, new MessageIdImpl(12, 4,1), 20, 1)
    print(endMessageId._1 + " "+ endMessageId._2)

  }


  def ser(a: AnyRef) =
    (new java.io.ObjectOutputStream(new java.io.ByteArrayOutputStream())).writeObject(a)

  test("offsetRange serialization check"){

    var offsetRangesPerPartitionMap: ju.Map[String, PulsarOffsetRange] = new ju.HashMap[String, PulsarOffsetRange]
    val pulsarOffsetRange: PulsarOffsetRange = PulsarOffsetRange(new MessageIdImpl(123, 24, -1), new MessageIdImpl(12, 343, 23), 234)
    offsetRangesPerPartitionMap.put("234",pulsarOffsetRange)
    ser(offsetRangesPerPartitionMap)
  }

  test("cred generation test"){
    val config=ViestiConfig("", "https://service.authn-prod.fkcloud.in","viesti-super-user-1", "eHtJjLIyDPtM8CiAuW3mf5nuY9OEs/YAkHPSrY6iaKoURz0v")
    config.buildClientCredentials("viesti-super-user-1", "eHtJjLIyDPtM8CiAuW3mf5nuY9OEs/YAkHPSrY6iaKoURz0v", "https://service.authn-prod.fkcloud.in").toExternalForm
  }
}

