package com.flipkart.utils

import java.text.SimpleDateFormat
import java.util.TimeZone

/**
 * Created by shivang.b on 12/21/15.
 */
object TimeFormatTransformation {

  def convertTime(time: String, initialTimeFormat: String, finalTimeFormat: String, timeZone: String): String = {
    val tz = TimeZone.getTimeZone(timeZone)
    val df = new SimpleDateFormat(finalTimeFormat)
    df.setTimeZone(tz)
    val dateToTransform = new SimpleDateFormat(initialTimeFormat).parse(time)
    val nowAsISO = df.format(dateToTransform)
    nowAsISO
  }

}
