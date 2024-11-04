package com.flipkart.service

/**
 * Created by sharma.varun on 17/10/15.
 */
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import com.flipkart.core.SignaturePayload
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.SignatureCassandraModel
import com.flipkart.utils.JsonUtility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object SignatureService {
  def saveDataToCassandra(repartition:Boolean, numPartitions:Int, data_rdd: DStream[(String,String)], zkProperties:Map[String, String])(
    implicit
    rwf: RowWriterFactory[SignatureCassandraModel]): Unit = {
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
    rwf: RowWriterFactory[SignatureCassandraModel]): Unit = {
    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val SIGNATURE_TABLE = "signature"
    try {
      implicit lazy val formats = org.json4s.DefaultFormats
      //parse the request and convert to cassandra model
      val signatureStream = rdd
        .map(x => JsonUtility.deserialize[SignaturePayload](x._2).convertToCassandraModel())
      //save to cassandra
      signatureStream.saveToCassandra(CASSANDRA_KEY_SPACE, SIGNATURE_TABLE, SignatureCassandraModel.getColumns)
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
