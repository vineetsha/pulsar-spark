package com.flipkart.service

import com.flipkart.client.BigfootClient
import com.flipkart.core.{EventBigfootPayload, EventHistoryPayload, Schema}
import com.flipkart.utils.Utility.{CompanyEstTime, getValue}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.JsonUtility
import org.apache.logging.log4j.Level.{ERROR, INFO, WARN}
import org.apache.spark.rdd.RDD

object EventBigfootServiceImpl extends BigfootService {
  override def saveDataToBigfoot[T](rdd: RDD[T], schema: Schema): Unit = {
    try {
      Logger.log(this.getClass, INFO, BaseSLog(s"saveEventToBigfoot 2 enter"))
      val bigfootPayloadMapped = rdd
        .map(x => JsonUtility.deserialize[EventHistoryPayload](getValue(x)).convertEventToBigfootModel(schema.version))
      filterAndSendDataToBigFoot(bigfootPayloadMapped, schema.name)
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in ingesting GeoTag to dart/ de-serializing RDD : " + e.getMessage, e))
        throw e
    }
  }

  def filterAndSendDataToBigFoot(bigfootPayloadMapped: RDD[EventBigfootPayload], entityName: String): Unit = {
    val good_data = bigfootPayloadMapped.filter(x => x.eventTime > CompanyEstTime)
    val bad_data = bigfootPayloadMapped.filter(x => x.eventTime < CompanyEstTime)
    bad_data.foreachPartition(partition => {
      if (partition.nonEmpty) {
        Logger.log(this.getClass, WARN, BaseSLog(s"Number of payload(s) having company inception pre-dates in event are" + partition.length))
      }
    })
    BigfootClient.postToBigfoot(good_data, entityName, "POST")
  }
}
