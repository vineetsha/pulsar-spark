package com.flipkart.model

import com.datastax.spark.connector.{ColumnName, SomeColumns}

/**
 * Created by vikas.gargay on 01/11/16.
 *

CREATE TABLE compass.smart_lookup_v1 (
    mmi_id text,
    doc_type text,
    ngram text,
    geohashes set<text>,
    PRIMARY KEY ((mmi_id, doc_type, ngram))
)

 */

case class SmartLookupModel(
                          var mmi_id:String,
                          var doc_type:String,
                          var ngram:String,
                          var geohashes:List[String])

object SmartLookupModel{
  def getColumns: SomeColumns ={
    SomeColumns(
      ColumnName("mmi_id"),
      ColumnName("doc_type"),
      ColumnName("ngram"),
      ColumnName("geohashes"))
  }

  override def toString = s"SmartLookupModel($getColumns)"
}