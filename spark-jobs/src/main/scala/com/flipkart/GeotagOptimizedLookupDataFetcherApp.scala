package com.flipkart


import com.datastax.spark.connector._
import com.flipkart.utils.HttpClient
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.{SparkConf, SparkContext}


object GeotagOptimizedLookupDataFetcherApp {

  def dump_geotag_optimized_lookup_data(addr_hash: Option[String], addr: Option[Vector[UDTValue]], attributes: Option[Vector[UDTValue]]): Unit = {
    if (addr.isDefined && addr_hash.isDefined && attributes.isDefined) {
      val headers = new java.util.HashMap[String, String]()
      val ingestor_url = "http://10.50.211.150:7000/write_geotag_optimized_lookup_data"
      headers.put("Content-Type", "application/json")
      val ingestor_params = new java.util.HashMap[String, String]()
      ingestor_params.put("addr_hash", addr_hash.get)
      val temp_addr = addr.get
      for (i <- 0 to temp_addr.length - 1) {
        ingestor_params.put(if (temp_addr(i).getStringOption("key").isDefined){temp_addr(i).getString("key")} else
        {return },
          if (temp_addr(i).getStringOption("value").isDefined){temp_addr(i).getString("value")} else
          {""})
      }
      val temp_attributes = attributes.get
      for (i <- 0 to temp_attributes.length - 1) {
        ingestor_params.put("lat", temp_attributes(i).getString("lat"))
        ingestor_params.put("lng", temp_attributes(i).getString("lng"))
        ingestor_params.put("time", temp_attributes(i).getString("time"))

        try {
          val response = HttpClient.INSTANCE.executeGet(ingestor_url, ingestor_params, headers)
        } catch {
          case e: Exception =>
            Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while docid ingestor: " + e.getMessage, e))
        }
      }
    }
  }


  def main(args: Array[String]): Unit = {

    val appName = "GeotagOptimizedLookupDataFetcherApp"
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

    val geotag_optimized_lookup_rdd = sc.cassandraTable("compass", "geotag_optimized_lookup").
      select("addr_hash", "addr", "attributes")

    val rdd_final = geotag_optimized_lookup_rdd.map(x => dump_geotag_optimized_lookup_data(
      x.getStringOption("addr_hash"), x.get[Option[Vector[UDTValue]]]("addr"),
      x.get[Option[Vector[UDTValue]]]("attributes")))

    rdd_final.count()
  }
}