package com.flipkart.service
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import com.flipkart.core.GeoTagPayload
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.{GeoTagLookupModel, GeoTagCassandraModel}
import com.flipkart.utils.JsonUtility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by sharma.varun on 08/10/15.
 */

object GeoTagService {
  def saveDataToCassandra(repartition:Boolean, numPartitions:Int, data_rdd: DStream[(String,String)], zkProperties:Map[String, String])(
    implicit
    rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
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
    rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val GEOTAG_TABLE = "geotag"
    lazy val GEOLOOKUP_TABLE = "geotag_lookup"
    try {
      implicit lazy val formats = org.json4s.DefaultFormats
      //parse the request and convert to cassandra model
      val geoTagStreamMap = rdd
        .map(x =>JsonUtility.deserialize[GeoTagPayload](x._2).convertToCassandraModel())
      //save to cassandra
      geoTagStreamMap.saveToCassandra(CASSANDRA_KEY_SPACE, GEOTAG_TABLE, GeoTagCassandraModel.getColumns)

      val geoLookupModel = rdd
        .map(x => JsonUtility.deserialize[GeoTagPayload](x._2).convertToGeoLookupModel())
      geoLookupModel.saveToCassandra(CASSANDRA_KEY_SPACE, GEOLOOKUP_TABLE,GeoTagLookupModel.getColumns)

      //commit the offsets once everything is done
      ZookeeperManager.updateOffsetsinZk(zkProperties, rdd)
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in saving to cassandra/updating offsets in zk: " + e.getMessage ,e))
        throw e;
    }
  }

}
