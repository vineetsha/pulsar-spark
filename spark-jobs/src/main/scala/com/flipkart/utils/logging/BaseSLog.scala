package com.flipkart.utils.logging

/**
 * Created by sharma.varun
 *
 * Each Logger Case Class, must have a proper toString Impl,
 * and according to rule_set which is defined in the corresponding logger
 */

trait StructuredLog {
  def message: String

  def error: Throwable
}


case class BaseSLog(message: String, error: Throwable = null,tag:String="DEFAULT") extends StructuredLog {

  override def toString: String = {
    //val outputStream = new ByteArrayOutputStream()
    //error.printStackTrace(new PrintStream(outputStream))
    tag +Logger.DELIMITER +  message +  Logger.DELIMITER //+ error.getLocalizedMessage + Log.DELIMITER  + outputStream.toString
  }
}

