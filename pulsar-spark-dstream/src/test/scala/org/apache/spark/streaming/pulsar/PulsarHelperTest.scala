package org.apache.spark.streaming.pulsar

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.mockito.MockitoSugar

import java.io.File
import java.{util => ju}
class PulsarHelperTest  extends SparkFunSuite with MockitoSugar{
  private val cachedPulsarAdmin=mock[CachedPulsarAdmin]

  private val objectMapper= new ObjectMapper()
  val conf=new ju.HashMap[String, AnyRef]()
  private val sparkConf=new SparkConf()
  private var topicName: String =_
  private var subscription: String=_



  override def beforeAll(): Unit = {
    super.beforeAll()
    CachedPulsarAdmin.setActivePulsarAdmin(cachedPulsarAdmin)
    topicName="persistent://tenant/ns/topic"
    subscription="pulsar-test/sub"
    sparkConf
      .set("spark.streaming.pulsar.maxRatePerPartition", "20")
      .set("spark.streaming.pulsar.avgMessagesPerEntry","1.0")

  }

  override def afterAll(): Unit = {
    super.afterAll()
    CachedPulsarAdmin.setActivePulsarAdmin(null)
  }

  /*
  1. New subscription. no message consumed yet, check mdp in both of the given cases
    1. LATEST
    2. EARLIEST

  2. Batching enabled and disabled
    1. start and end belongs to Same ledger
    2. start and end belongs to different ledger.

  3. partition deleted.

  4. out of sync

  5. test case for nThMessage


   */

  test("getStartingEntry"){


    val stats=objectMapper.readValue(new File("src/test/resources/helper/topic-stats.json"), classOf[PartitionedTopicInternalStats])

    when(cachedPulsarAdmin.getPartitionedInternalStats(topicName)).thenReturn(stats)

    val mdp=PulsarHelper(conf).getMarkDeletePosititon(stats.partitions.get(topicName+"-partition-0"), topicName+"-partition-0", SubscriptionInitialPosition.Earliest,subscription)
    //Cursor Stat Available for parition-0
    assert(mdp.getLedgerId == 792443)
    assert(mdp. getEntryId == 5562)
    val mdp1=PulsarHelper(conf).getMarkDeletePosititon(stats.partitions.get(topicName+"-partition-1"), topicName+"-partition-1", SubscriptionInitialPosition.Earliest,subscription)
    //Cursor Stat Available for parition-1
    assert(mdp1.getEntryId == -1)
  }

  test("computeMaxEntriesToReadPerPartition"){
    val ppm=new ju.HashMap[String, PulsarPartitionMetadata]()
    val partition=List("persistent://tenant/ns/topic-partition-0", "persistent://tenant/ns/topic-partition-1", "persistent://tenant/ns/topic-partition-2")

    ppm.put(partition(0), PulsarPartitionMetadata(Seq(), 100, null))
    ppm.put(partition(1), PulsarPartitionMetadata(Seq(), 0, null))
    ppm.put(partition(2), PulsarPartitionMetadata(Seq(), 1000, null))

    //When estimated rate is less than maxRatePerPartition
    val partitionToEntryMap1=PulsarHelper(conf).computeMaxEntriesToReadPerPartition(Some(10), ppm, 5000, new DefaultPerPartitionConfig(sparkConf))
    assert(partitionToEntryMap1.get(partition(0)).get==5)
    assert(partitionToEntryMap1.get(partition(1)).get==0)
    assert(partitionToEntryMap1.get(partition(2)).get==46)


    //When estimated rate is less than maxRatePerPartition
    val partitionToEntryMap2=PulsarHelper(conf).computeMaxEntriesToReadPerPartition(Some(40), ppm, 5000, new DefaultPerPartitionConfig(sparkConf))
    assert(partitionToEntryMap2.get(partition(0)).get==19)
    assert(partitionToEntryMap2.get(partition(1)).get==0)
    assert(partitionToEntryMap2.get(partition(2)).get==100)


    //When estimated rate is not available
    val partitionToEntryMap3=PulsarHelper(conf).computeMaxEntriesToReadPerPartition(Option.empty, ppm, 5000, new DefaultPerPartitionConfig(sparkConf))
    assert(partitionToEntryMap3.get(partition(0)).get==100)
    assert(partitionToEntryMap3.get(partition(1)).get==0)
    assert(partitionToEntryMap3.get(partition(2)).get==100)

  }


  test("Test: getOffsetRange(), For newly created subcription, with EARLIEST initial position"){
    val offsetRangeMap=new ju.HashMap[String, PulsarOffsetRange]()
    val ppc=new DefaultPerPartitionConfig(sparkConf)
    val batchInterval=5000L
    val estimatedRateLimit= Option.empty
    val stats=objectMapper.readValue(new File("src/test/resources/helper/topic-stats-new-earliest-subscription.json"), classOf[PartitionedTopicInternalStats])
    when(cachedPulsarAdmin.getPartitionedInternalStats(topicName)).thenReturn(stats)


    PulsarHelper(conf).getOffsetRange(topicName, subscription, SubscriptionInitialPosition.Earliest, offsetRangeMap, ppc, batchInterval,estimatedRateLimit)

  }


//  test("Test: getOffsetRange(), For newly created subcription, with LATEST initial position"){
//
//
//    PulsarHelper(conf).getOffsetRange(topicName, subscription, subscriptionInitialPosition, offsetRangeMap, ppc, batchInterval,estimatedRateLimit)
//  }
//
//  test("Test: getOffsetRange(), For cursor in b/w ledgers"){
//
//
//    PulsarHelper(conf).getOffsetRange(topicName, subscription, subscriptionInitialPosition, offsetRangeMap, ppc, batchInterval,estimatedRateLimit)
//  }
//
//  test("Test: getOffsetRange(), No msg available to consume"){
//
//
//    PulsarHelper(conf).getOffsetRange(topicName, subscription, subscriptionInitialPosition, offsetRangeMap, ppc, batchInterval,estimatedRateLimit)
//  }
//
//  test("Test: getOffsetRange(), on ledger deletion"){
//
//
//    PulsarHelper(conf).getOffsetRange(topicName, subscription, subscriptionInitialPosition, offsetRangeMap, ppc, batchInterval,estimatedRateLimit)
//  }


}


