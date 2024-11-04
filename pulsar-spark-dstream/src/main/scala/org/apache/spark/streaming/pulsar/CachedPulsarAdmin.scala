package org.apache.spark.streaming.pulsar

import org.apache.arrow.util.VisibleForTesting
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.pulsar.PulsarContants.AuthPluginClassName

import java.util.concurrent.atomic.AtomicReference
import java.{util => ju}
import scala.collection.JavaConverters.mapAsScalaMapConverter
private[pulsar] class CachedPulsarAdmin(config: ju.Map[String, AnyRef]) extends Logging {

  private val pulsarAdmin: PulsarAdmin = {
    val pulsarServiceUrl = PulsarProvider.getServiceUrl(config)
    val adminConf =
      PulsarConfigUpdater("pulsarAdminCache", config.asScala.toMap, PulsarContants.FilteredKeys)
        .rebuild()
    logInfo(s"admin Conf = ${adminConf}")

    val builder = PulsarAdmin.builder()
    try {
      builder
        .serviceHttpUrl(pulsarServiceUrl)

      // Set authentication parameters.
      if (adminConf.containsKey(AuthPluginClassName)) {
        builder.authentication(
          adminConf.get(AuthPluginClassName).toString,
          adminConf.get(PulsarContants.AuthParams).toString)
      }

      val pulsaradmin: PulsarAdmin = builder.build()
      logInfo(
        s"Created a new instance of PulsarAdmin for serviceHttpUrl = $pulsarServiceUrl,"
          + s" adminConf = $adminConf.")

      pulsaradmin
    } catch {
      case e: Throwable =>
        logError(
          s"Failed to create Pulsaradmin to serviceUrl ${pulsarServiceUrl}"
            + s" using admin conf ${adminConf}",
          e)
        throw e
      case e: Exception =>
        logError(
          s"Failed to create Pulsaradmin to serviceUrl ${pulsarServiceUrl}"
            + s" using admin conf ${adminConf}",
          e)
        throw e
    }

  }

  def resetCursor(topic: String, fQsubscription: String, msgId: MessageId): Unit={
    pulsarAdmin.topics().resetCursor(topic, fQsubscription, msgId, true)
  }

  def createSubscriptionIfNotAvailable(topic: String, fQsubscription: String, startingMessageId: MessageId): Unit={
    logInfo("Checking whether subscription is present or not")
    if (!pulsarAdmin.topics().getSubscriptions(topic).contains(fQsubscription)) {

      //create subscription if not already in pulsar
      logInfo("Create subscription if not already in pulsar")
      pulsarAdmin.topics().createSubscription(topic, fQsubscription, startingMessageId)
    }
    else{
      logInfo("Subscription already in pulsar")
    }
  }

  def getPartitionedInternalStats(topicName: String): PartitionedTopicInternalStats = {
    pulsarAdmin.topics().getPartitionedInternalStats(topicName)
  }



}
private[pulsar] object CachedPulsarAdmin extends Logging {
  private val activePulsarAdmin: AtomicReference[CachedPulsarAdmin] =
    new AtomicReference[CachedPulsarAdmin](null)
  private val PULSAR_ADMIN_CONSTRUCTOR_LOCK = new Object()

  private def getCachedPulsarAdmin(config: ju.Map[String, AnyRef]): CachedPulsarAdmin = {
    new CachedPulsarAdmin(config)
  }

  def getOrCreate(config: ju.Map[String, AnyRef]): CachedPulsarAdmin = {
    PULSAR_ADMIN_CONSTRUCTOR_LOCK.synchronized {
      if (activePulsarAdmin.get() == null) {
        setActivePulsarAdmin(getCachedPulsarAdmin(config))
      }
      activePulsarAdmin.get()
    }
  }
  @VisibleForTesting
  private[spark] def setActivePulsarAdmin(cachedPulsarAdmin: CachedPulsarAdmin): Unit = {
    PULSAR_ADMIN_CONSTRUCTOR_LOCK.synchronized {
      activePulsarAdmin.set(cachedPulsarAdmin)
    }
  }
  private[spark] def clearActivePulsarAdmin(): Unit = {
    PULSAR_ADMIN_CONSTRUCTOR_LOCK.synchronized {
      activePulsarAdmin.set(null)
    }
  }


}
