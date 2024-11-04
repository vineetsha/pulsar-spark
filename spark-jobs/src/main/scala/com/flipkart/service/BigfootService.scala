package com.flipkart.service

import com.flipkart.core.{GeoTagBigfootPayload, Schema}
import org.apache.spark.rdd.RDD

trait BigfootService{
  def saveDataToBigfoot[T](rdd: RDD[T],schema:Schema)
}
