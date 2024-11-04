package com.flipkart.model

import com.datastax.spark.connector.{ColumnName, SomeColumns, UDTValue}

/**
 * Created by sharma.varun on 17/10/15.
 */
case class GeoTagCassandraModel(device_id:String,
                                date:String,
                                tag_id:String,
                                time:String,
                                lat:Double,
                                lng:Double,
                                alt:Double,
                                src:String,
                                _type:String,
                                addr_hash:String,
                                addr_full:String,
                                addr:List[UDTValue],
                                attributes:List[UDTValue],
                                address_id:String,
                                account_id:String
                               )

object GeoTagCassandraModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("device_id"),
      ColumnName("date"),
      ColumnName("tag_id"),
      ColumnName("time"),
      ColumnName("lat"),
      ColumnName("lng"),
      ColumnName("alt"),
      ColumnName("src"),
      ColumnName("type" ,Some("_type")),
      ColumnName("addr_hash"),
      ColumnName("addr_full"),
      ColumnName("addr"),
      ColumnName("attributes"),
      ColumnName("address_id"),
      ColumnName("account_id")
    )
  }
}