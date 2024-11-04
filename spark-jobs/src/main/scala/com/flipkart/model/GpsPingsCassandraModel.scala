package com.flipkart.model

import com.datastax.spark.connector.{ColumnName, SomeColumns, UDTValue}

/**
  * Created by raunaq.singh on 22/06/20.
  */
case class GpsPingsCassandraModel(entity_id:String,
                                  entity_type:String,
                                  tenant:String,
                                  accuracy:Double,
                                  battery:Double,
                                  speed:Double,
                                  altitude:Double,
                                  latitude:Double,
                                  longitude:Double,
                                  source:String,
                                  client_timestamp:BigInt,
                                  attributes:List[UDTValue])

object GpsPingsCassandraModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("entity_id"),
      ColumnName("entity_type"),
      ColumnName("tenant"),
      ColumnName("accuracy"),
      ColumnName("battery"),
      ColumnName("speed"),
      ColumnName("altitude"),
      ColumnName("latitude"),
      ColumnName("longitude"),
      ColumnName("source"),
      ColumnName("client_timestamp"),
      ColumnName("attributes"))
  }
}