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
object AddressIngestionApp {

  def main(args: Array[String]) {

    val appName = "Geolookup Address Update"
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
      val rdd = sc.cassandraTable("compass", "geotag").select("addr_hash", "addr")

      val rdd2 = rdd.map(x => (x.getString("addr_hash"), x.getList[UDTValue]("addr")))

      rdd.unpersist()

      val rdd4 = rdd2.combineByKey(
        (x: Vector[UDTValue]) => List(x),
        (acc: List[Vector[UDTValue]], x) => x :: acc,
        (acc1: List[Vector[UDTValue]],
         acc2: List[Vector[UDTValue]]) => acc1 ::: acc2
      )

      val rdd1 = sc.cassandraTable("compass", "geotag_optimized_lookup").select("addr_hash", "addr")
      val rdd1_2 = rdd1.map(x=>(x.getString("addr_hash"), x.getList[UDTValue]("addr")))
      val rdd1_3 = rdd1_2.map(x=>(x._1, x._2.toList.map(y=> x._2)))
      rdd1.unpersist()
      val rdd_union = rdd4.union(rdd1_3).reduceByKey((x,y) => x ::: y).map(x => (x._1, x._2)).filter(x => x._2.nonEmpty)
      val rdd1_4 = rdd_union.map(x => (x._1, x._2.last))
      rdd1_4.saveToCassandra("compass", "geotag_optimized_lookup", SomeColumns("addr_hash", "addr"))
    }
    catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing AddressApp: " + e.getMessage, e))
        throw e
    }
    finally{
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping AddressApp"))
        sc.stop()
        Logger.log(this.getClass, ERROR, BaseSLog(s"AddressApp stopped"))
      }
    }

  }

  def addAddressOneByOne(addressHash: String, connector: CassandraConnector, sc: SparkContext): ResultSet = {
    val select = new SimpleStatement("SELECT addr FROM compass.geotag where addr_hash ='"+addressHash+"';")
    connector.withSessionDo(session => {
      val addressRow = session.execute(select)
      if(addressRow!=null) {
        val address = addressRow.one()
        if (address != null) {
          val addr = address.getObject("addr").asInstanceOf[java.util.List[UDTValue]]
          val insert = new SimpleStatement("INSERT INTO compass.geotag_optimized_lookup ( addr_hash, addr ) VALUES ('" + addressHash + "', " + addr + ");")
          return session.execute(insert)
        }
      }
      return addressRow
    })
  }

}