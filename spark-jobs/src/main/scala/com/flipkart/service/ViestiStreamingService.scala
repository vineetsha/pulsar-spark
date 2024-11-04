package com.flipkart.service

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import com.flipkart.config.Config
import com.flipkart.core.{GeoTagPayload, Schema}
import com.flipkart.model.GeoTagCassandraModel
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.JsonUtility
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pulsar.SparkPulsarMessage
import org.apache.spark.util.LongAccumulator



object ViestiStreamingService extends StreamingService[SparkPulsarMessage] {

  override def processAndSaveToBigFoot(repartition: Boolean, numPartitions: Int, data_rdd: DStream[SparkPulsarMessage],schema:Schema)(bigfootService: BigfootService): Unit = {
    Logger.log(this.getClass, INFO, BaseSLog(s"Save data to bigfoot"))
    repartitionStream(data_rdd, repartition, numPartitions).foreachRDD(rdd => {
      bigfootService.saveDataToBigfoot(rdd,schema)
    })
  }


   def processAndSaveToCassandra(repartition: Boolean, numPartitions: Int, data_rdd: DStream[SparkPulsarMessage], clusterScoreConfig: Map[String, String], ssc: StreamingContext)(implicit rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
    repartitionStream(data_rdd, repartition, numPartitions).foreachRDD(rdd => {
      saveData(rdd.map(x => new String(x.data)), clusterScoreConfig, ssc)
    })
  }



  def saveData(rdd: RDD[String], clusterScoreConfig: Map[String, String], ssc: StreamingContext)(
    implicit
    rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
    lazy val CASSANDRA_KEY_SPACE = Config.getProperty("spark.streamingAppV3.ingestion.keyspace", "compass_nfr")
    lazy val CASSANDRA_GEOTAG_OPT_READ_KEYSPACE=Config.getProperty("spark.streamingAppV3.geotagOpt.readKeyspace","compass")
    lazy val GEOTAG_TABLE = "geotag"
    lazy val GEOTAG_OPTIMIZED_LOOKUP_TABLE = "geotag_optimized_lookup"
    try {
      implicit lazy val formats = org.json4s.DefaultFormats
      //parse the request and convert to cassandra model
      val geoTagStreamMap: RDD[GeoTagCassandraModel] = rdd
        .map(x => JsonUtility.deserialize[GeoTagPayload](x).convertToCassandraModel())

      //save to cassandra main table
      //Logger.log(this.getClass, INFO, BaseSLog(s"Saving to Geotag"))
      //Logger.log(this.getClass, INFO, BaseSLog("Saving:" + GeoTagCassandraModel.getColumns.toString))
      //geoTagStreamMap.saveToCassandra(CASSANDRA_KEY_SPACE, GEOTAG_TABLE, GeoTagCassandraModel.getColumns)

      //save to cassandra lookup table
      Logger.log(this.getClass, INFO, BaseSLog(s"Processing Started For geotagOptimizedLookup"))
      GeoTagOptimizedService.saveDataToCassandraForLookup(geoTagStreamMap, ssc, clusterScoreConfig, CASSANDRA_KEY_SPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE,CASSANDRA_GEOTAG_OPT_READ_KEYSPACE)
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in saving to cassandra: " + e.getMessage, e))
        throw e;
    }
  }

}
