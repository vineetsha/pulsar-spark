package com.flipkart.model

import com.datastax.spark.connector.{ColumnName, SomeColumns, UDTValue}

/**
 * Created by sharma.varun on 17/10/15.
 */
case class EventHistoryCassandraModel(device_id:String,
                                date:String,
                                time:String,
                                lat:Double,
                                lng:Double,
                                alt:Double,
                                src:String,
                                _type:String,
                                attributes:List[UDTValue]){
  def convertToEvent():EventCassandraModel = {
    new EventCassandraModel(this.device_id,
      this.time,
      this.lat,
      this.lng,
      this.alt,
      this.src,
      this._type,
      this.attributes)
  }
}

case class EventCassandraModel(device_id:String,
                               time:String,
                               lat:Double,
                               lng:Double,
                               alt:Double,
                               src:String,
                               _type:String,
                               attributes:List[UDTValue])

object EventHistoryCassandraModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("device_id"),
      ColumnName("date"),
      ColumnName("time"),
      ColumnName("lat"),
      ColumnName("lng"),
      ColumnName("alt"),
      ColumnName("src"),
      ColumnName("type" ,Some("_type")),
      ColumnName("attributes"))
  }
}

object EventCassandraModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("device_id"),
      ColumnName("time"),
      ColumnName("lat"),
      ColumnName("lng"),
      ColumnName("alt"),
      ColumnName("src"),
      ColumnName("type" ,Some("_type")),
      ColumnName("attributes"))
  }
}