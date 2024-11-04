package com.flipkart.core

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.datastax.spark.connector.UDTValue
import com.fasterxml.jackson.annotation.JsonProperty
import com.flipkart.model.{GeoTagLookupModel, GeoTagCassandraModel}
import com.flipkart.utils.{KeyValuePairGenerator, TimeFormatTransformation}

import scala.collection.mutable.ListBuffer

/**
 * Created by sharma.varun on 17/10/15.
 */

class GeoTagPayload(var device_id:String,
var date:String,
var tag_id:String,
var time:String,
var latitude:Double,
var longitude:Double,
var altitude:Double,
var source:String,
@JsonProperty("type")
var _type:String,
var address_hash:String,
var address_full:String,
var address:List[Map[String, String]],
var attributes:Map[String, String], var address_id:String, var account_id:String) extends Serializable {

  private def convertAttributes(attributes:Map[String,String]):
  List[UDTValue] ={
    var list = new ListBuffer[UDTValue]
    for ((key, value) <- attributes) {
      var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
      list += udtValue
    }
    list.toList
  }

  private def convertAttributesForBigfoot(attributes:Map[String,String]):
  List[Map[String, String]] ={
    var list = new ListBuffer[Map[String, String]]
    for ((key, value) <- attributes) {
      var attributeMap = Map("key" -> key, "value" -> value)
      list += attributeMap
    }
    list.toList
  }

  private def filterAttributesForGeoLookup(attributes:Map[String,String]):
  List[UDTValue] ={
    var list = new ListBuffer[UDTValue]
    for ((key, value) <- attributes) {
      if (key=="delivery_verification_code_type"||key=="accuracy_level"||key=="is_same_spot_delivery"){
        var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
        list += udtValue
      }
    }
    list.toList
  }

  def convertAddress(address:List[Map[String,String]]):
  List[UDTValue] ={
    var listBuffer = new ListBuffer[UDTValue]
    for (addr <- address) {
      for ((key, value) <- addr) {
        var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
        listBuffer += udtValue
      }
    }
    listBuffer.toList
  }

  private def convertAddressforBigfoot(address:List[Map[String,String]]):
    Map[String, String] ={
    var addrMap = Map[String,String]()
    for (addr <- address) {
      for ((key, value) <- addr) {
        addrMap += (key -> value)
      }
    }
    addrMap
  }

  def convertToCassandraModel(): GeoTagCassandraModel ={
    new GeoTagCassandraModel(this.device_id,
      this.date,
      this.tag_id,
      this.time,
      this.latitude,
      this.longitude,
      this.altitude,
      this.source,
      this._type,
      this.address_hash,
      this.address_full,
      convertAddress(this.address),
      convertAttributes(this.attributes),
      this.address_id,
      this.account_id)
  }

  def convertGeotagToBigfootModel(schemaVersion:String): GeoTagBigfootPayload ={
    new GeoTagBigfootPayload(TimeFormatTransformation.convertTime(this.time, "yyyy-MM-dd HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "IST")+"-"+this.device_id+"-"+this.attributes("shipment_id"),
      new GeoTagData(this.device_id,
        this.date,
        this.tag_id,
        TimeFormatTransformation.convertTime(this.time, "yyyy-MM-dd HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "IST"),
        this.latitude,
        this.longitude,
        this.altitude,
        this.source,
        this._type,
        this.address_hash,
        this.address_full,
        convertAddressforBigfoot(this.address),
        KeyValuePairGenerator.fromMapToList(this.attributes)),
      TimeFormatTransformation.convertTime(this.time, "yyyy-MM-dd HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "IST"),
    schemaVersion)
  }

  def convertToGeoLookupModel(): GeoTagLookupModel ={
    new GeoTagLookupModel(this.address_hash,
    this.tag_id,
    this.time,
    filterAttributesForGeoLookup(this.attributes),
    this.device_id,
    this.latitude,
    this.longitude)
  }

}