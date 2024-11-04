package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.pulsar.PulsarContants.{LEDGER_ENTRY_SEPARATOR, LEDGER_OPENED_STATE}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.control.Breaks.{break, breakable}

case class PulsarLedger(ledgerId: Long, entries: Long) {}

object PulsarLedger extends Logging {
  private def isLedgerOpen(stats: PersistentTopicInternalStats): Boolean = {
    if (!stats.state.equals(LEDGER_OPENED_STATE)) { //Check  If LedgerOpenedState marked at ledger stat
      return false;
    }
    if (stats.ledgers.asScala.last.entries > 0) { // In cursor stats if entry count for the latest ledger is updated then it is closed.
      return false
    }

    val splits = stats.lastConfirmedEntry.split(LEDGER_ENTRY_SEPARATOR)
    val ledgerId = splits(0).toLong

    if (ledgerId != stats.ledgers.asScala.last.ledgerId) { // If last added entry into latest ledger then it is opened.
      return false
    }
    true
  }

  def createLedgers(stats: PersistentTopicInternalStats): Seq[PulsarLedger] = {
    var ledgers = stats.ledgers.asScala.map(l => PulsarLedger(l.ledgerId, l.entries))
    if (isLedgerOpen(stats)) {
      val splits = stats.lastConfirmedEntry.split(LEDGER_ENTRY_SEPARATOR)
      val ledgerId = splits(0).toLong
      val entries = splits(1).toLong + 1
      val myLedger = PulsarLedger(ledgerId, entries)
      ledgers = ledgers.dropRight(1)
      ledgers.append(myLedger)
    }
    val ans = ledgers.filter(ledger => ledger.entries > 0)
    logDebug(ledgers.map(ledger => "LedgerId: " + ledger.ledgerId + " has " + ledger.entries + " entries").mkString("\n"))
    ans
  }

  //Compute backlog of entries excluding fromMessageId
  def computeBacklog(ledgers: Seq[PulsarLedger], fromMessageIdOptional: Option[MessageIdImpl]): Long = {
    if (fromMessageIdOptional.isEmpty || ledgers.isEmpty) {
      return 0L;
    }
    var fromMessageId = fromMessageIdOptional.get
    if (fromMessageId.getLedgerId < ledgers.head.ledgerId) { // In case of ledger deleted fromMessageId could be stale.
      fromMessageId = new MessageIdImpl(ledgers.head.ledgerId, -1, fromMessageId.getPartitionIndex)
    }
    else if (fromMessageId.getLedgerId > ledgers.last.ledgerId) { // If given entry lies outside the ledgers available.
      return 0L
    }

    var backlog = 0L
    var foundLastLedger = false
    for (l <- ledgers) {
      if (foundLastLedger) {
        backlog += l.entries
      }
      if (l.ledgerId.equals(fromMessageId.getLedgerId)) {
        if (fromMessageId.getEntryId == -1) {
          backlog += l.entries
        }
        else {
          backlog += l.entries - fromMessageId.getEntryId - 1
        }
        foundLastLedger = true
      }
    }
    backlog
  }


  def lastEntryOfLedger(ledger: PulsarLedger) = ledger.entries - 1

  /*
  Returns Last message Id to fetch for required number of entries with the batch size.
   fromMessageId is inclusive in requiredEntries.
   */
  def getEndMessageId(ledgers: Seq[PulsarLedger], startMessageId: MessageIdImpl, requiredEntries: Long, partition: Int): (MessageIdImpl, Long) = {

    require(requiredEntries!=0, "Total entry count required should non zero.")
    if(ledgers.head.ledgerId<startMessageId.getLedgerId){
      return getEndMessageId(ledgers.drop(1), startMessageId, requiredEntries, partition)
    }

    var entriesToFetched: Long = requiredEntries
    var endMessageId: MessageIdImpl = null
    var totalEntriesFetched: Long = 0
    var startingEntryId = startMessageId.getEntryId

    breakable {
      for (ledger <- ledgers) {
        val pendingEntiresInCurrentLedger = ledger.entries - startingEntryId
        var entriesFetchedFromCurrentLedger: Long = 0
        var endMessageEntryId: Long = -1

        if (entriesToFetched < pendingEntiresInCurrentLedger) {
          endMessageEntryId = startingEntryId + entriesToFetched - 1 //-1 because end entry id is inclusive
          entriesFetchedFromCurrentLedger = entriesToFetched
        }
        else {
          // For entriesToFetched >= pendingEntiresInCurrentLedger, entire ledger entries should be fetched.
          endMessageEntryId = lastEntryOfLedger(ledger)
          entriesFetchedFromCurrentLedger = pendingEntiresInCurrentLedger
        }
        totalEntriesFetched = totalEntriesFetched + entriesFetchedFromCurrentLedger
        entriesToFetched = entriesToFetched - entriesFetchedFromCurrentLedger
        endMessageId = new MessageIdImpl(ledger.ledgerId, endMessageEntryId, partition)

        // Reset startingEntryId for next batch
        startingEntryId = 0
        if (entriesToFetched == 0) {
          break()
        }
      }

    }
    (endMessageId, totalEntriesFetched)
  }

  /*
    Return next offset to given currentOffset.
   */
  def getNextMessageId(pulsarLedgers: Seq[PulsarLedger], currentOffset: MessageIdImpl): Option[MessageIdImpl] = {
    if (pulsarLedgers.isEmpty) {
      Option.empty
    }
    else if(pulsarLedgers.head.ledgerId<currentOffset.getLedgerId){
      getNextMessageId(pulsarLedgers.drop(1), currentOffset)
    }
    else {
      val currentLedger = pulsarLedgers.head
      if (currentOffset.getLedgerId == currentLedger.ledgerId) {
        if (currentOffset.getEntryId + 1 >= currentLedger.entries) { //if given entry is the last entry of current ledger, move to next ledger
          // here two possible may exist: next ledger may contain entries or may not, hence calling recursive again to check next ledger.
          getNextMessageId(pulsarLedgers.drop(1), currentOffset)
        } else {
          Some(new MessageIdImpl(currentOffset.getLedgerId, currentOffset.getEntryId + 1, currentOffset.getPartitionIndex))
        }
      }
      else {
        val head = pulsarLedgers.head
        if (head.entries > 0) {
          Some(new MessageIdImpl(head.ledgerId, 0, currentOffset.getPartitionIndex))
        }
        else { //empty ledger may come hence drop it and move to next ledger
          getNextMessageId(pulsarLedgers.drop(1), currentOffset)
        }
      }
    }
  }

}
