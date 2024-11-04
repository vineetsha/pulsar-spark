package com.flipkart.utils

import java.time.format.DateTimeFormatter

object DateUtils {
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
}
