package com.flipkart.model

import com.datastax.spark.connector.{UDTValue, ColumnName, SomeColumns}

/**
 * Created by sourav.r on 18/12/15.
 *
 *CREATE TABLE compass.geocode_validation (
   addr_id text,
   addr text,
   g_lat double,
   g_lng double,
   m_lat double,
   m_lng double,
   u_lat double,
   u_lng double,
   attributes list<frozen<mytuple>>
   PRIMARY KEY ((addr_id))
   )
 */
case class GeoCodeValidationModel(addr_id:String,
                                  addr:String,
                                  g_lat:Option[Double],
                                  g_lng:Option[Double],
                                  m_lat:Option[Double],
                                  m_lng:Option[Double],
                                  u_lat:Option[Double],
                                  u_lng:Option[Double],
                                  attributes:List[UDTValue])
object GeoCodeValidationModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("addr_id"),
      ColumnName("addr"),
      ColumnName("g_lat"),
      ColumnName("g_lng"),
      ColumnName("m_lat"),
      ColumnName("m_lng"),
      ColumnName("u_lat"),
      ColumnName("u_lng"),
      ColumnName("attributes"))
  }

  override def toString = s"GeoCodeValidationModel($getColumns)"
}

