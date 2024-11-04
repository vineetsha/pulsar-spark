package com.flipkart.config

import com.flipkart.InitializableApp
import com.flipkart.utils.JsonUtility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._

/**
 * Created by sharma.varun
 */
object Config extends InitializableApp{

  private val binder = { init() }

  def init(){
    if (isInitialized.compareAndSet(false, true)) {
     Logger.log(this.getClass, INFO, BaseSLog("Config Initing..."))
     Logger.log(this.getClass, INFO, BaseSLog(s"Loaded Configs >>>>  ${JsonUtility.serialize(ConfigService.getConfigurationSnapshot(ConfigBuckets.COMPASS))}"))
    }
  }


  def getProperty( propPath:String, defaultValue:String):String = ConfigService.getProperty(ConfigBuckets.COMPASS, propPath, defaultValue)

  def shutdown() = noop
}
