package com.flipkart.model

import com.datastax.spark.connector.{ColumnName, SomeColumns, UDTValue}

/**
 * Created by shivang.b on 12/10/16.
 */
case class GeoTagLookupModel(addr_hash:String,
                          tag_id:String,
                          time:String,
                          attributes:List[UDTValue],
                          device_id:String,
                          lat:Double,
                          lng:Double)




object GeoTagLookupModel {
  def getColumns: SomeColumns = {
    SomeColumns(ColumnName("addr_hash"),
      ColumnName("tag_id"),
      ColumnName("time"),
      ColumnName("attributes"),
      ColumnName("device_id"),
      ColumnName("lat"),
      ColumnName("lng"))
  }
}
