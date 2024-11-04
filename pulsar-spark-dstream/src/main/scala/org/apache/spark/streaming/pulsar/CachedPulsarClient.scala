package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.impl.PulsarClientImpl
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.pulsar.PulsarContants.AuthPluginClassName

import java.util.concurrent.atomic.AtomicReference
import java.{util => ju}
import scala.collection.JavaConverters.mapAsScalaMapConverter

private[pulsar] object CachedPulsarClient extends Logging {
  private val activePulsarClient: AtomicReference[PulsarClientImpl] =
    new AtomicReference[PulsarClientImpl](null)
  private val PULSAR_CLIENT_CONSTRUCTOR_LOCK = new Object()

  def getOrCreate(config: ju.Map[String, AnyRef]): PulsarClientImpl = {
    PULSAR_CLIENT_CONSTRUCTOR_LOCK.synchronized {
      if (activePulsarClient.get() == null) {
        setActivePulsarClient(getPulsarClient(config))
      }
      activePulsarClient.get()
    }
  }
  private[spark] def setActivePulsarClient(pulsarClient: PulsarClient): Unit = {
    PULSAR_CLIENT_CONSTRUCTOR_LOCK.synchronized {
      activePulsarClient.set(pulsarClient.asInstanceOf[PulsarClientImpl])
    }
  }

  private def getPulsarClient(config: ju.Map[String, AnyRef]): PulsarClient = {
    val pulsarServiceUrl = PulsarProvider.getServiceUrl(config)
    val clientConf =
      PulsarConfigUpdater("pulsarClientCache", config.asScala.toMap, PulsarContants.FilteredKeys)
        .rebuild()
    logInfo(s"client Conf = ${clientConf}")

    val builder = PulsarClient.builder()
    try {
      builder
        .serviceUrl(pulsarServiceUrl)

      // Set authentication parameters.
      if (clientConf.containsKey(AuthPluginClassName)) {
        builder.authentication(
          clientConf.get(AuthPluginClassName).toString,
          clientConf.get(PulsarContants.AuthParams).toString)
      }

      val pulsarClient: PulsarClient = builder.build()
      logInfo(
        s"Created a new instance of PulsarClient for serviceUrl = $pulsarServiceUrl,"
          + s" clientConf = $clientConf.")

      pulsarClient
    } catch {
      case e: Throwable =>
        logError(
          s"Failed to create PulsarClient to serviceUrl ${pulsarServiceUrl}"
            + s" using client conf ${clientConf}",
          e)
        throw e
      case e: Exception =>
        logError(
          s"Failed to create Pulsar Clientn to serviceUrl ${pulsarServiceUrl}"
            + s" using client  conf ${clientConf}",
          e)
        throw e
    }

  }

  private[spark] def clearActivePulsarClient(): Unit = {
    PULSAR_CLIENT_CONSTRUCTOR_LOCK.synchronized {
      if(activePulsarClient.get()!=null){
        val pulsarClientImpl=activePulsarClient.get()
        if(!pulsarClientImpl.isClosed){
          pulsarClientImpl.close()
        }
      }
      activePulsarClient.set(null)
    }
  }


}
