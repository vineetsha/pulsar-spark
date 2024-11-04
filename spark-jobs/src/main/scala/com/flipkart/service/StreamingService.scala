package com.flipkart.service

import com.datastax.spark.connector.writer.RowWriterFactory
import com.flipkart.core.Schema
import com.flipkart.model.GeoTagCassandraModel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.LongAccumulator

trait StreamingService[T] {
  def repartitionStream(data_rdd: DStream[T], repartition: Boolean, numPartitions: Int) = {
    if (repartition) data_rdd.repartition(numPartitions) else data_rdd
  }
  def processAndSaveToBigFoot(repartition:Boolean, numPartitions:Int, data_rdd: DStream[T],schema:Schema)(bigfootService: BigfootService)

  def processAndSaveToCassandra(repartition:Boolean, numPartitions:Int, data_rdd: DStream[T], clusterScoreConfig:Map[String, String], ssc: StreamingContext)(
    implicit
    rwf: RowWriterFactory[GeoTagCassandraModel])



}
