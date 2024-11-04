package com.flipkart.config

import com.flipkart.InitializableApp
import com.flipkart.config.ConfigBuckets.ConfigBucket
import com.flipkart.kloud.config.{ConfigClient, DynamicBucket}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.Utility
import org.apache.logging.log4j.Level._

import scala.collection.JavaConverters._

/**
 * Created by sharma.varun
 */
object ConfigService extends InitializableApp {

  var buckets: Map[ConfigBucket, Seq[DynamicBucket]] = Map()

  protected var client: ConfigClient = null

  val (configServiceHost, configServicePort): (String, Int) = {
    val configServiceEndpoint = Utility.getEnvironmentVariable("CONFIG_SERVICE_ENDPOINT").getOrElse("10.24.0.32")
    val configServicePort = Integer.parseInt(Utility.getEnvironmentVariable("CONFIG_SERVICE_PORT").getOrElse("80"))
    (configServiceEndpoint,configServicePort)
  }


  override def init(): Unit = this.synchronized {
    if (isInitialized.compareAndSet(false, true)) {
      Utility.doRetry(3) {
        println(s"Config Service Attempting $configServiceHost:$configServicePort")
        client = new ConfigClient(configServiceHost, configServicePort, 1, 30000)
        Logger.log(this.getClass, INFO, BaseSLog(s"Loading Config Buckets: ${ConfigBuckets.values.toList}"))
        ConfigBuckets.values.foreach(bucketConf => {
          val bucketNames = bucketConf.toString.split(',').toSeq
          buckets += bucketConf -> bucketNames.map(bucketName => client.getDynamicBucket(bucketName))
        })
        Logger.log(this.getClass, INFO, BaseSLog("Config Buckets Loaded"))
      }
    } else {
      Logger.log(this.getClass, INFO, BaseSLog(s"${this.getClass.getName} is already initialized"))
    }
  }

  /**
   * This is autoInit App
   */
  private val binder = {
    init()
  }

  def getConfigurationSnapshot(bucketName: ConfigBucket): Map[String, AnyRef] = {
    buckets(bucketName).map(_.getKeys.asScala).reduceLeft(_ ++ _).toMap
  }

  def getProperty(bucketName: ConfigBucket, key: String, defaultValue: String): String = {
    buckets.get(bucketName) match {
      case Some(bk) =>
        var value: String = bk.map(_.getString(key)).reduceLeftOption((leftValue, rightValue) => (leftValue, rightValue) match {
          case x if rightValue != null => rightValue
          case y if leftValue != null => leftValue
          case _ => null
        }).orNull

        if (Utility.isNullOrEmpty(value)) {
          Logger.log(this.getClass, DEBUG, BaseSLog(s"Invalid Key Name [ $key ], returning default value."))
          value = defaultValue
        }
        value
      case None =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Invalid Bucket Name, returning default value."))
        defaultValue
    }
  }

  override def shutdown(): Unit = try {
    if (client != null)
      client.shutdown()
  } catch {
    case e: Throwable =>
      Logger.log(this.getClass, ERROR, BaseSLog(s"Config Service Shutdown Error", error = e))
  }
}

object ConfigBuckets extends Enumeration {

  type ConfigBucket = Value
  val COMPASS = Value(s"fk-ekl-compass-spark-jobs-${Utility.getEnvironmentVariable("APP_ENV").getOrElse("hyd")}")
}
