package com.flipkart.service

import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.writer.RowWriterFactory
import com.flipkart.core.{GeoTagPayload, Schema}
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.GeoTagCassandraModel
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.{ConfigMapUtil, JsonUtility}
import org.apache.logging.log4j.Level.{ERROR, INFO}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object KafkaStreamingService extends StreamingService[(String, String)] {

  override def processAndSaveToBigFoot(repartition: Boolean, numPartitions: Int, data_rdd: DStream[(String, String)],schema:Schema)(bigfootService: BigfootService): Unit = {

    Logger.log(this.getClass, INFO, BaseSLog(s"saveEventToBigfoot enter"))
    repartitionStream(data_rdd, repartition, numPartitions).foreachRDD(rdd => {
      bigfootService.saveDataToBigfoot(rdd,schema)
      ZookeeperManager.updateOffsetsinZk(ConfigMapUtil.zkPropertiesBigfoot, rdd)
    })
  }

  def processAndSaveToCassandra(repartition:Boolean, numPartitions:Int, data_rdd: DStream[(String,String)], clusterScoreConfig:Map[String, String], ssc: StreamingContext)(
    implicit
    rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
    repartitionStream(data_rdd, repartition, numPartitions).foreachRDD(rdd => {
      saveDataToCassandra( rdd, ConfigMapUtil.zkPropertiesCompass, clusterScoreConfig, ssc)
    })
  }

  def saveDataToCassandra(rdd: RDD[(String, String)], zkProperties: Map[String, String], clusterScoreConfig: Map[String, String], ssc: StreamingContext)(
    implicit
    rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val GEOTAG_TABLE = "geotag"
    lazy val GEOTAG_OPTIMIZED_LOOKUP_TABLE = "geotag_optimized_lookup"
    try {
      implicit lazy val formats = org.json4s.DefaultFormats
      //parse the request and convert to cassandra model
      val geoTagStreamMap: RDD[GeoTagCassandraModel] = rdd
        .map(x => JsonUtility.deserialize[GeoTagPayload](x._2).convertToCassandraModel())
      //save to cassandra
      Logger.log(this.getClass, INFO, BaseSLog(s"Saving to Geotag"))
      geoTagStreamMap.saveToCassandra(CASSANDRA_KEY_SPACE, GEOTAG_TABLE, GeoTagCassandraModel.getColumns)

      //save to cassandra
      Logger.log(this.getClass, INFO, BaseSLog(s"Processing Started For geotagOptimizedLookup"))
      GeoTagOptimizedService.saveDataToCassandraForLookup(geoTagStreamMap, ssc, clusterScoreConfig, CASSANDRA_KEY_SPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE,CASSANDRA_KEY_SPACE)

      //commit the offsets once everything is done
      Logger.log(this.getClass, INFO, BaseSLog(s"Committing Offsets"))

      ZookeeperManager.updateOffsetsinZk(zkProperties, rdd)

    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in saving to cassandra/updating offsets in zk: " + e.getMessage, e))
        throw e;
    }
  }


}
