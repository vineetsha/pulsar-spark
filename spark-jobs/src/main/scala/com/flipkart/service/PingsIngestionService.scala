package com.flipkart.service

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import com.flipkart.core.GpsPingsPayload
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.GpsPingsCassandraModel
import com.flipkart.utils.JsonUtility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level.{ERROR, INFO}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object PingsIngestionService {
  def saveDataToCassandra(repartition:Boolean, numPartitions:Int, data_rdd: DStream[(String,String)], zkProperties:Map[String, String], ssc: StreamingContext)(
    implicit
    rwf: RowWriterFactory[GpsPingsCassandraModel]): Unit = {
    var data_stream:DStream[(String, String)] = null
    if (repartition) {
      data_stream = data_rdd.repartition(numPartitions)
    }
    else{
      data_stream = data_rdd
    }
    data_stream.foreachRDD(rdd=> {
      saveDataToCassandra(numPartitions, rdd, zkProperties, ssc)
    })
  }

  def saveDataToCassandra(numPartitions:Int, rdd: RDD[(String,String)], zkProperties:Map[String, String], ssc: StreamingContext)(
    implicit
    rwf: RowWriterFactory[GpsPingsCassandraModel]): Unit = {
    lazy val CASSANDRA_KEY_SPACE = "gps"
    lazy val PINGS_TABLE = "pings"
    try {
      implicit lazy val formats = org.json4s.DefaultFormats
      //parse the request and convert to cassandra model
      val gpsPingsStreamMap: RDD[GpsPingsCassandraModel] = rdd
        .map(x =>JsonUtility.deserialize[List[GpsPingsPayload]](x._2).map(y => y.convertToCassandraModel())).flatMap(z => z)
      //save to cassandra
      Logger.log(this.getClass, INFO, BaseSLog(s"Saving to Pings"))
      gpsPingsStreamMap.saveToCassandra(CASSANDRA_KEY_SPACE, PINGS_TABLE, GpsPingsCassandraModel.getColumns)

      //commit the offsets once everything is done
      Logger.log(this.getClass, INFO, BaseSLog(s"Committing Offsets"))
      ZookeeperManager.updateOffsetsinZk(zkProperties, rdd)
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in saving to cassandra/updating offsets in zk: " + e.getMessage ,e))
        throw e;
    }
  }
}