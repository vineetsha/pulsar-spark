package com.flipkart.service
import com.flipkart.client.BigfootClient
import com.flipkart.core.{GeoTagBigfootPayload, GeoTagPayload, Schema}
import com.flipkart.utils.Utility.{CompanyEstTime, getValue}
import com.flipkart.utils.JsonUtility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level.{ERROR, INFO, WARN}
import org.apache.spark.rdd.RDD

object GeoTagBigfootServiceImpl extends BigfootService {
  override def saveDataToBigfoot[T](rdd: RDD[T],schema:Schema): Unit = {
    try {
      Logger.log(this.getClass, INFO, BaseSLog(s"saveGeoTagToBigfoot 2 enter"))
      val bigfootPayloadMapped = rdd
        .map(x => JsonUtility.deserialize[GeoTagPayload](getValue(x)).convertGeotagToBigfootModel(schema.version))
      filterAndSendDataToBigFoot(bigfootPayloadMapped,schema.name)
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in ingesting GeoTag to dart/ de-serializing RDD : " + e.getMessage, e))
        throw e
    }
  }

   def filterAndSendDataToBigFoot(bigfootPayloadMapped: RDD[GeoTagBigfootPayload],schemaName:String): Unit = {
    val good_data = bigfootPayloadMapped.filter(x=>x.eventTime > CompanyEstTime)
    val bad_data = bigfootPayloadMapped.filter(x=>x.eventTime < CompanyEstTime)
     bad_data.foreachPartition(partition => {
       if (partition.nonEmpty) {
         Logger.log(this.getClass, WARN, BaseSLog(s"Number of payload(s) having company inception pre-dates in GeoTag are" + partition.length))
       }
     })
    val filtered_good_data = good_data.filter(x=> x.data.address.nonEmpty)
    BigfootClient.postToBigfoot(filtered_good_data, schemaName, "POST")
  }

}
