package com.flipkart.model

import com.datastax.spark.connector.{ColumnName, SomeColumns, UDTValue}

/**
 * Created by sourav.r on 18/12/15.
 */
case class GeoTagDeviationModel(hub_id:String,
                                date:String,
                                addr_hash:String,
                                tag_id:String,
                                c_lat:Double,
                                c_lng:Double,
                                c_acc:Double,
                                g_lat:Double,
                                g_lng:Double,
                                addr_full:String)

object GeoTagDeviationModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("hub_id"),
      ColumnName("date"),
      ColumnName("addr_hash"),
      ColumnName("tag_id"),
      ColumnName("c_lat"),
      ColumnName("c_lng"),
      ColumnName("c_acc"),
      ColumnName("g_lat"),
      ColumnName("g_lng"),
      ColumnName("addr_full"))
  }

  override def toString = s"GeoTagDeviationModel($getColumns)"
}
