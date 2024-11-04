package org.apache.spark.streaming.pulsar

import org.apache.spark.internal.Logging

import java.{util => ju}
import scala.collection.JavaConverters.mapAsJavaMapConverter


/**
 * Class to conveniently update pulsar config params, while logging the changes.
 */
case class PulsarConfigUpdater(
                                module: String,
                                pulsarParams: Map[String, Object],
                                blacklistedKeys: Set[String] = Set(),
                                keysToHideInLog: Set[String] = Set(PulsarContants.AuthParams))
  extends Logging {

  private val map = new ju.HashMap[String, Object](pulsarParams.asJava)

  def set(key: String, value: Object): this.type = {
    set(key, value, map)
  }

  def set(key: String, value: Object, map: ju.Map[String, Object]): this.type = {
    if (blacklistedKeys.contains(key)) {
      logInfo(s"$module: Skip '$key'")
    } else {
      map.put(key, value)
      logInfo(
        s"$module: Set '$key' to " +
          s"'${printConfigValue(key, Option(value))}'," +
          s" earlier value: '${printConfigValue(key, pulsarParams.get(key))}'")
    }
    this
  }

  def build(): ju.Map[String, Object] = map

  def rebuild(): ju.Map[String, Object] = {
    val map = new ju.HashMap[String, Object]()
    pulsarParams map { case (k, v) =>
      set(k, v, map)
    }
    map
  }

  private val HideCompletelyLimit = 6
  private val ShowFractionOfHiddenValue = 1.0 / 3.0
  private val CompletelyHiddenMessage = "...<completely hidden>..."

  private def printConfigValue(key: String, maybeVal: Option[Object]): String = {
    val value = maybeVal.map(_.toString).getOrElse("")
    if (keysToHideInLog.contains(key)) {
      if (value.length < HideCompletelyLimit) {
        return CompletelyHiddenMessage
      } else {
        return s"${value.take((value.length * ShowFractionOfHiddenValue).toInt)}..."
      }
    }

    value
  }
}

