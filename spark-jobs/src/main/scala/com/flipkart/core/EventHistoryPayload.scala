package com.flipkart.core

import com.datastax.spark.connector.UDTValue
import com.fasterxml.jackson.annotation.JsonProperty
import com.flipkart.model.EventHistoryCassandraModel
import com.flipkart.utils.{KeyValuePairGenerator, TimeFormatTransformation}

import scala.collection.mutable.ListBuffer

/**
  * Created by sharma.varun on 17/10/15.
  */

class EventHistoryPayload(var device_id:String,
                     var date:String,
                     var time:String,
                     var latitude:Double,
                     var longitude:Double,
                     var altitude:Double,
                     var source:String,
                     @JsonProperty("type")
                     var _type:String,
                     var attributes:Map[String, String]) extends Serializable{

  private def convertAttributes(attributes:Map[String,String]):
  List[UDTValue] ={
    var list = new ListBuffer[UDTValue]
    for ((key, value) <- attributes) {
      var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
      list += udtValue
    }
    list.toList
  }

  def convertToCassandraModel(): EventHistoryCassandraModel = {
    new EventHistoryCassandraModel(this.device_id,
      this.date,
      this.time,
      this.latitude,
      this.longitude,
      this.altitude,
      this.source,
      this._type,
      convertAttributes(this.attributes))
  }

  def convertEventToBigfootModel(schemaVersion: String): EventBigfootPayload ={
    new EventBigfootPayload(this.device_id+"-"+TimeFormatTransformation.convertTime(this.time, "yyyy-MM-dd HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "IST"),
      new EventData(this.device_id,
        this.date,
        TimeFormatTransformation.convertTime(this.time, "yyyy-MM-dd HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "IST"),
        this.latitude,
        this.longitude,
        this.altitude,
        this.source,
        this._type,
        KeyValuePairGenerator.fromMapToList(this.attributes)),
      TimeFormatTransformation.convertTime(this.time, "yyyy-MM-dd HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "IST"),
      schemaVersion
      )
  }
}
