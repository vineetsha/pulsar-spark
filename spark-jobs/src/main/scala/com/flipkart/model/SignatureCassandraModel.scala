package com.flipkart.model

import java.nio.ByteBuffer

import com.datastax.spark.connector.{ColumnName, SomeColumns}

/**
 * Created by sharma.varun on 17/10/15.
 */
case class SignatureCassandraModel(tag_id:String,
                                   src:String,
                                   _type:String,
                                   time:String,
                                   signature:ByteBuffer)

object SignatureCassandraModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("tag_id"),
      ColumnName("src"),
      ColumnName("type" ,Some("_type")),
      ColumnName("time"),
      ColumnName("signature"))
  }
}