package org.apache.spark.streaming.pulsar

import org.apache.commons.lang3.StringUtils
import org.apache.pulsar.client.api.{MessageId, SubscriptionInitialPosition}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.pulsar.PulsarConfigurationUtils._
import org.apache.spark.streaming.pulsar.PulsarContants._

import java.{util => ju}
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `map AsScala`}

private[pulsar] object PulsarProvider extends Logging {

  def getDefaultMsgId(readerParams: ju.Map[String, AnyRef])=if(readerParams.getOrElse(subscriptionInitialPositionKey, SubscriptionInitialPosition.Latest).equals(SubscriptionInitialPosition.Earliest)) MessageId.earliest else MessageId.latest

  def getDefaultMsgId(subscriptionInitialPosition: SubscriptionInitialPosition)=if(subscriptionInitialPosition.equals(SubscriptionInitialPosition.Earliest)) MessageId.earliest else MessageId.latest


  def getClientParams(parameters: ju.Map[String, AnyRef]): ju.Map[String, AnyRef] = {
    getModuleParams(parameters, PulsarClientOptionKeyPrefix, clientConfKeys)
  }

   def getReaderParams(parameters: ju.Map[String, AnyRef]): ju.Map[String, AnyRef] = {
    getModuleParams(parameters, PulsarReaderOptionKeyPrefix, readerConfKeys)
  }

   def getModuleParams(connectorConfiguration: ju.Map[String, AnyRef],
                       modulePrefix: String,
                       moduleKeyLookup: ju.Map[String, String]): ju.Map[String, AnyRef] = {
    val moduleParameters = connectorConfiguration.keySet
      .filter(_.startsWith(modulePrefix))
      .map { k =>
        k.drop(modulePrefix.length) -> connectorConfiguration.get(k)
      }
      .toMap
     moduleParameters.map { case (k, v) =>
      moduleKeyLookup.getOrElse(
        k,
        throw new IllegalArgumentException(s"$k not supported by pulsar")) -> v
    }.asJava
  }

  def paramsToPulsarConf(module: String, params: ju.Map[String, AnyRef]): ju.Map[String, Object] = {
    PulsarConfigUpdater(module, params.asScala.toMap).rebuild()
  }

   def getPredefinedSubscription(parameters: ju.Map[String, AnyRef]): String = {
    parameters.get(PredefinedSubscription).toString
  }

   def getServiceUrl(parameters: ju.Map[String, AnyRef]): String = {
    parameters.get(ServiceUrlOptionKey).toString
  }

  def pollTimeoutMs(): Int = {
    SparkEnv.get match {
      case null => 60000
      case g: SparkEnv => (g.conf.getTimeAsSeconds("spark.network.timeout", "120s") * 1000).toInt
    }

  }

  def validatePredefinedSubsription(subscription: AnyRef) = {
    if(subscription==null){
      throw new IllegalArgumentException("Predefined subscription should be present")
    }
    else if(StringUtils.isEmpty(subscription.toString)){
      throw new IllegalArgumentException("PredefinedSubscription should not be null or empty")
    }
    else if(!subscription.toString.contains("/")){
      throw new IllegalArgumentException("PredefinedSubscription should be fully qualifed <Tenant-name>/<subscription-name>")
    }
  }

  def validateReaderOptions(caseInsensitiveParams: ju.Map[String, AnyRef]): ju.Map[String, AnyRef] = {
    // validate topic options
    val topicOptions = caseInsensitiveParams.filter { case (k, _) =>
      TopicOptionKeys.contains(k)
    }.toSeq
    if (topicOptions.isEmpty || topicOptions.size > 1) {
      throw new IllegalArgumentException(
        "You should specify topic using one of the topic options: "
          + TopicOptionKeys.mkString(", "))
    }
    topicOptions.head match {
      case ("topicNames", value) =>
        if (value.toString.contains(",")) {
          throw new IllegalArgumentException(
            """Cannot read from multiple topic read""")
        } else if (value.toString.trim.isEmpty) {
          throw new IllegalArgumentException("No topic is specified")
        }
    }
    validatePredefinedSubsription(caseInsensitiveParams.get(PulsarContants.PredefinedSubscription))
    caseInsensitiveParams
  }

  def validateClientParams(clientParams: ju.Map[String, AnyRef]):ju.Map[String,AnyRef] ={
    if (!clientParams.contains(ServiceUrlOptionKey)) {
      throw new IllegalArgumentException(s"$ServiceUrlOptionKey must be specified")
    }
    clientParams
  }
  def getTopic(readerParams: ju.Map[String, AnyRef]): String= readerParams.get(TopicSingle).toString

   def prepareConfForReader(parameters: ju.Map[String, AnyRef])
  : (ju.Map[String, Object], ju.Map[String, Object]) = {

    var clientParams = getClientParams(parameters)
    val readerParams = getReaderParams(parameters)
    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.reader", readerParams))
  }

  def getSubscriptionIntitialPosition(readerParams: ju.Map[String, AnyRef]): SubscriptionInitialPosition={
    readerParams.getOrDefault(PulsarContants.subscriptionInitialPositionKey, SubscriptionInitialPosition.Latest).asInstanceOf[SubscriptionInitialPosition]
  }




}
