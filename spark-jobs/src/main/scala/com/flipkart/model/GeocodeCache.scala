package com.flipkart.model
import com.datastax.spark.connector.{UDTValue, ColumnName, SomeColumns}

/**
 * Created by sourav.r on 17/08/16.
 *
 * CREATE TABLE compass.geocode_cache (
    addr_hash text PRIMARY KEY,
    tracking_id text,
    addr_text text,
    addr list<frozen<mytuple>>,
    g_lat double,
    g_lng double,
    create_time timestamp
)
 */
case class GeocodeCacheModel(addr_hash:String,
                             tracking_id:String,
                             addr_text:String,
                             addr:List[UDTValue],
                             geocode_result:String,
                             create_time: java.util.Date)

object GeocodeCacheModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("addr_hash"),
      ColumnName("addr"),
      ColumnName("addr_text"),
      ColumnName("create_time"),
      ColumnName("geocode_result"),
      ColumnName("tracking_id")
    )
  }
  override def toString = s"GeocodeCacheModel($getColumns)"
}
