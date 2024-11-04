package com.flipkart.core

import com.datastax.spark.connector.UDTValue
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.flipkart.model.GpsPingsCassandraModel

import scala.collection.mutable.ListBuffer

/**
  * Created by raunaq.singh on 22/06/20.
  **/

  @JsonIgnoreProperties(ignoreUnknown = true)
  class GpsPingsPayload(var entity_id:String,
                        var entity_type:String,
                        var tenant:String,
                        var accuracy:Double,
                        var battery:Double,
                        var speed:Double,
                        var altitude:Double,
                        var latitude:Double,
                        var longitude:Double,
                        var source:String,
                        var device_id:String,
                        var odometer:Double,
                        var gps_status:Int,
                        var ignition:Int,
                        var direction:Int,
                        var received_at_in_millis:BigInt,
                        var attributes:Map[String,String]) extends Serializable {

  private def convertAttributes(attributes:Map[String,String], device_id:String, odometer:Double, gps_status:Int,
                                ignition:Int, direction:Int): List[UDTValue] ={
    var list = new ListBuffer[UDTValue]
    if(null != attributes) {
      for ((key, value) <- attributes) {
        var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
        list += udtValue
      }
    }

    if(null != device_id)
      list += UDTValue.fromMap(Map("key" -> "device_id", "value" -> device_id))
    if(odometer != 0)
      list += UDTValue.fromMap(Map("key" -> "odometer", "value" -> odometer.toString))
    if(gps_status != 0)
      list += UDTValue.fromMap(Map("key" -> "gps_status", "value" -> gps_status.toString))
    if(ignition != 0)
      list += UDTValue.fromMap(Map("key" -> "ignition", "value" -> ignition.toString))
    if(direction != 0)
      list += UDTValue.fromMap(Map("key" -> "direction", "value" -> direction.toString))
    list.toList
  }

  def convertToCassandraModel(): GpsPingsCassandraModel ={
    new GpsPingsCassandraModel(this.entity_id,
      this.entity_type,
      this.tenant,
      this.accuracy,
      this.battery,
      this.speed,
      this.altitude,
      this.latitude,
      this.longitude,
      this.source,
      this.received_at_in_millis,
      convertAttributes(this.attributes, this.device_id, this.odometer, this.gps_status, this.ignition, this.direction))
  }
}

