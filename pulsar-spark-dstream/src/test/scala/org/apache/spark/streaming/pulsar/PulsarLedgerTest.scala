package org.apache.spark.streaming.pulsar

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats
import org.apache.spark.SparkFunSuite

import java.io.File

class PulsarLedgerTest extends SparkFunSuite{
  private val objectMapper= new ObjectMapper()

  test("createLedgersTest"){
    val partitionedTopic="persistent://tenant/ns/topic"
    val subscription="pulsar-test/sub"
    val stats=objectMapper.readValue(new File("src/test/resources/helper/topic-stats.json"), classOf[PartitionedTopicInternalStats])
    val ledgers0=PulsarLedger.createLedgers(stats.partitions.get(TopicPartition.getPartitionName(partitionedTopic, 0)))

    assert(ledgers0.size==2)
    assert(ledgers0.head.entries==5)

    val ledgers1=PulsarLedger.createLedgers(stats.partitions.get(TopicPartition.getPartitionName(partitionedTopic, 1)))
    assert(ledgers1.size==0) //Due to filtering of empty ledgers.
  }

  test("computeBacklogTest"){
    val partitionedTopic="persistent://tenant/ns/topic"
    val subscription="pulsar-test/sub"
    val stats=objectMapper.readValue(new File("src/test/resources/helper/topic-stats.json"), classOf[PartitionedTopicInternalStats])
    val ledgers=PulsarLedger.createLedgers(stats.partitions.get(TopicPartition.getPartitionName(partitionedTopic, 0)))

    assert(PulsarLedger.computeBacklog(ledgers, Some(new MessageIdImpl(793109, -1, 0)))==10)
    assert(PulsarLedger.computeBacklog(ledgers, Some(new MessageIdImpl(793109, 0, 0)))==9)
    assert(PulsarLedger.computeBacklog(ledgers, Some(new MessageIdImpl(793109, 4, 0)))==5)
    assert(PulsarLedger.computeBacklog(ledgers, Some(new MessageIdImpl(791109, 2354, 0)))==10)
  }

  test("nextMessageIdTest"){
    val ledgers=Seq(PulsarLedger(12, 1), PulsarLedger(14,0), PulsarLedger(19, 10), PulsarLedger(20,1))
    assert(PulsarLedger.getNextMessageId(ledgers, new MessageIdImpl(11, 203, -1)).get.equals(new MessageIdImpl(12, 0, -1)))
    assert(PulsarLedger.getNextMessageId(ledgers, new MessageIdImpl(12, 0, -1)).get.equals(new MessageIdImpl(19, 0, -1)))
    assert(PulsarLedger.getNextMessageId(ledgers, new MessageIdImpl(22, 0, -1)).isEmpty)
    assert(PulsarLedger.getNextMessageId(ledgers, new MessageIdImpl(19, 2, -1)).get.equals(new MessageIdImpl(19, 3, -1)))
    assert(PulsarLedger.getNextMessageId(ledgers, new MessageIdImpl(20, 0, -1)).isEmpty)
    assert(PulsarLedger.getNextMessageId(ledgers, new MessageIdImpl(14, -1, -1)).get.equals(new MessageIdImpl(19, 0, -1)))
    assert(PulsarLedger.getNextMessageId(ledgers, new MessageIdImpl(13, -1, -1)).get.equals(new MessageIdImpl(19, 0, -1)))

  }

  test("getEndMessageId"){
    val ledgers=Seq(PulsarLedger(12, 1),PulsarLedger(13, 102), PulsarLedger(14,0), PulsarLedger(19, 10), PulsarLedger(20,1))
    // expected entries across ledgers
    val offsetRange1=PulsarLedger.getEndMessageId(ledgers, new MessageIdImpl(12, 0, -1),10, -1 )
    assert(offsetRange1._1.equals(new MessageIdImpl(13, 8, -1)))
    assert(offsetRange1._2.equals(10L))

    // expected count > total entries available
    val offsetRange2=PulsarLedger.getEndMessageId(ledgers, new MessageIdImpl(12, 0, -1),200, -1 )
    assert(offsetRange2._1.equals(new MessageIdImpl(20, 0, -1)))
    assert(offsetRange2._2.equals(114L))

    // expected entries within single ledger
    val offsetRange3=PulsarLedger.getEndMessageId(ledgers, new MessageIdImpl(12, 0, -1),1, -1 )
    assert(offsetRange3._1.equals(new MessageIdImpl(12, 0, -1)))
    assert(offsetRange3._2.equals(1L))


    // expected entries across ledgers with empty ledgers in b/w
    val offsetRange4=PulsarLedger.getEndMessageId(ledgers, new MessageIdImpl(13, 100, -1),10, -1 )
    assert(offsetRange4._1.equals(new MessageIdImpl(19, 7, -1)))
    assert(offsetRange4._2.equals(10L))


  }



}
