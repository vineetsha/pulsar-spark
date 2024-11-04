package com.flipkart.service

/**
 * Created by sharma.varun on 17/10/15.
 */

import java.text.SimpleDateFormat

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import com.flipkart.core.EventHistoryPayload
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.{EventCassandraModel, EventHistoryCassandraModel}
import com.flipkart.utils.JsonUtility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object EventService {
  def saveDataToCassandra(repartition:Boolean, numPartitions:Int, data_rdd: DStream[(String,String)], zkProperties:Map[String, String])(
    implicit
    rwf: RowWriterFactory[EventCassandraModel]): Unit = {
    var data_stream:DStream[(String, String)] = null
    if (repartition) {
      data_stream = data_rdd.repartition(numPartitions)
    }
    else{
      data_stream = data_rdd
    }
    data_stream.foreachRDD(rdd=> {
      saveDataToCassandra(numPartitions,rdd,zkProperties)
    })
  }
  def saveDataToCassandra(numPartitions:Int, rdd: RDD[(String,String)], zkProperties:Map[String, String])(
    implicit
    rwf: RowWriterFactory[EventCassandraModel]): Unit = {

    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val EVENT_TABLE = "event"
    lazy val EVENT_HISTORY_TABLE = "event_history"
    try{
      implicit lazy val formats = org.json4s.DefaultFormats
      //parse the request and convert to cassandra model
      val eventHistoryStream = rdd
        .map(x =>JsonUtility.deserialize[EventHistoryPayload](x._2).convertToCassandraModel())

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssz")
      val eventStream = eventHistoryStream
        .map(x=> x.convertToEvent())
        //finding the latest event from history
        .map(x => ((x.device_id, x.src, x._type), x))
        .reduceByKey((x, y) => if (dateFormat.parse(x.time).compareTo(dateFormat.parse(y.time)) > 0) x else y
        ).map(x => x._2)
      //save to cassandra
      eventHistoryStream.saveToCassandra(CASSANDRA_KEY_SPACE, EVENT_HISTORY_TABLE, EventHistoryCassandraModel.getColumns)
      eventStream.saveToCassandra(CASSANDRA_KEY_SPACE, EVENT_TABLE, EventCassandraModel.getColumns)
      //commit the offsets once everything is done
      ZookeeperManager.updateOffsetsinZk(zkProperties, rdd)
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in saving to cassandra/updating offsets in zk: " + e.getMessage, e))
        throw e;
    }
  }
}
