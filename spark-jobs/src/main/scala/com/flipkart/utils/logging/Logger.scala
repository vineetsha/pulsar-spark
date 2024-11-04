package com.flipkart.utils.logging

import org.apache.logging.log4j.Level
import org.slf4j.{Logger, LoggerFactory}
import org.apache.logging.log4j.Level._

/**
 * Created by sharma.varun on 02/09/2015.
 */
object Logger {

  val DELIMITER = "||"

  object FILE extends Enumeration {
    type FILE = String
    val DEFAULT = new String("DEFAULT")
  }

  def mlogger(filename: String): Logger = {
    LoggerFactory.getLogger(filename)
  }

  def log(filename: String, level: Level, obj: StructuredLog): Unit = {
    logger(filename, level, obj)
  }
  def log(filename: Class[_], level: Level, obj: StructuredLog): Unit = {
    logger(filename.getName, level, obj)
  }

  def logger(filename: String, level: Level, obj: StructuredLog): Unit = {
    level match {
      case ERROR =>
        mlogger(filename).error(obj.toString, obj.error)
      case WARN =>
        mlogger(filename).warn(obj.toString, obj.error)
      case INFO =>
        mlogger(filename).info(obj.toString, obj.error)
      case _ =>
        mlogger(filename).debug(obj.toString, obj.error)
    }
  }


}

