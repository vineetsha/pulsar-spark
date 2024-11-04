package com.flipkart

import com.datastax.driver.core.{ResultSet, SimpleStatement}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by akshay.rm on 11/09/19.
  */
object GeotagDataDeletionApp {

  def main(args: Array[String]) {

    val deleteAllDataBeforeDate = "2017-01-01"

    Logger.log(this.getClass, INFO, BaseSLog(s"Deleting all Data from Geotag Before : $deleteAllDataBeforeDate"))

    val appName = "Geolookup Data Deletion"
    val sparkMaster = "spark://10.52.18.196:7077"
    val sparkMaxCores = "20"
    val sparkExecutorMemory = "15g"
    val cassandraHost = "10.52.178.198,10.52.178.198,10.50.226.208,10.51.130.240,10.52.210.209,10.50.178.228,10.50.34.232,10.52.130.185"
    val datacenter = "datacenter3"
    val casssandraInputSplitSize = "67108864"

    Logger.log(this.getClass, INFO, BaseSLog(s"Creating Spark Context of app: $appName"))

    val conf = new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cores.max", sparkMaxCores)
      .set("spark.executor.memory", sparkExecutorMemory)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.input.split.size_in_mb", casssandraInputSplitSize)
      .set("spark.cassandra.connection.local_dc", datacenter)

    val sc = new SparkContext(conf)

    try {
      val rdd = sc.cassandraTable("compass", "geotag").select( "device_id", "date")
      val rdd2 = rdd.map(x => (x.getString("device_id"), x.getString("date"))).filter(x=> x._2 < deleteAllDataBeforeDate)
      val cc: CassandraConnector = CassandraConnector(sc.getConf)
      //val rdd_geotag_del = sc.parallelize(rdd2.take(1))
      val resp = rdd2.map(x => deleteByDeviceIdDateGeoTag(x._1, x._2, cc))
      resp.count()
    }
    catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing Data Deletion: " + e.getMessage, e))
        throw e
    }
    finally{
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping Data Deletion App"))
        sc.stop()
        Logger.log(this.getClass, ERROR, BaseSLog(s"Data Deletion App stopped"))
      }
    }

  }

  def deleteByAddressHash(addressHash: String, connector: CassandraConnector) : ResultSet = {
    val delete = new SimpleStatement("DELETE FROM compass.geotag where addr_hash ='"+addressHash+"';")
    connector.withSessionDo(session => {
      return session.execute(delete)
    })
  }

  def deleteByDeviceIdDateGeoTag(deviceId: String, date: String, connector: CassandraConnector) : ResultSet = {
    val delete = new SimpleStatement("DELETE FROM compass.geotag where device_id ='"+deviceId+"' and date ='"+date+"';")
    connector.withSessionDo(session => {
      return session.execute(delete)
    })
  }

}