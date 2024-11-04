package com.flipkart.client

import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.{ConfigMapUtil, HttpClient, JsonUtility}
import org.apache.http.HttpStatus
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD

import java.util.UUID

object BigfootClient {

  def postToBigfoot[T]( good_data: RDD[T], entityName: String, httpMethod: String) {
    Logger.log(this.getClass, INFO, BaseSLog(s"postToBigfoot enter"))
    good_data.foreachPartition(partition => {
      Logger.log(this.getClass, INFO, BaseSLog(s"entering each partition"))
      partition.grouped(ConfigMapUtil.configMap.apply("bigfootGroupAmount").toInt).foreach(partition => {
        if (partition.nonEmpty) {
          ingestPartition(JsonUtility.serialize(Map(entityName -> partition.toList)),entityName, httpMethod)
        }
        else {
          Logger.log(this.getClass, INFO, BaseSLog(s"partition empty"))
        }
      })
    })
  }
  def ingestPartition(bigfootJsonList: String,entityName: String,httpMethod:String): Unit ={
    Logger.log(this.getClass, INFO, BaseSLog(s"enter bigfoot post method"))
    val msg_id = UUID.randomUUID().toString
    val group_id = UUID.randomUUID().toString
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    headers.put("X_RESTBUS_MESSAGE_ID", msg_id)
    headers.put("X_RESTBUS_GROUP_ID", group_id)
    headers.put("X_RESTBUS_HTTP_URI", ConfigMapUtil.configMap.apply("bigfootDartEndpoint"))
    headers.put("X_RESTBUS_HTTP_METHOD", httpMethod)
    val response = HttpClient.INSTANCE.executePost(ConfigMapUtil.configMap.apply("varadhiUrl"), bigfootJsonList, headers)
    Logger.log(this.getClass, INFO, BaseSLog(s"Bigfoot " + entityName + "post response headers: " + response.getStatusCode + " " + response.getHeaders))
    Logger.log(this.getClass, INFO, BaseSLog(s"Bigfoot " + entityName + "post response body: " + response.getResponseBody))
    if (response.getStatusCode != HttpStatus.SC_OK) {
      Logger.log(this.getClass, ERROR, BaseSLog(s"Error in Bigfoot " + entityName + "Dart ingestion. Returned status code " + response.getStatusCode))
      throw new RuntimeException
    }
  }

}


