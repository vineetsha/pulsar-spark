package com.flipkart.batch

/**
 * Command to run this spark job:
 * /usr/share/spark/bin/spark-submit --class "GeoTagMigration" --total-executor-cores 10 --master spark://<spark-ip:port> --conf spark.cassandra.connection.host=<connector-ip> --conf spark.cassandra.input.split.size_in_mb=67108864 --executor-memory 2G /usr/share/spark/jars/fk-ekl-spark-jobs.jar <source-table> <destination-table>
 */


import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by shivang.b on 7/27/15.
 */
object GeoTagMigration {

  def main (args: Array[String]) {
    if (args.length < 2){
      System.err.println("Usage: Source Table --> Destination Table")
      System.exit(1)
    }

    val CASSANDRA_KEY_SPACE = "compass"
    val sc = new SparkContext(new SparkConf().setAppName("GeoTagMigration"))

    val rdd = sc.cassandraTable(CASSANDRA_KEY_SPACE,args(0)).select("device_id", "time", "attributes", "lat", "lng", "addr_hash", "tag_id")
    //  rdd.take(10).foreach(println)

    rdd.saveToCassandra(CASSANDRA_KEY_SPACE,args(1),SomeColumns("device_id", "time", "attributes", "lat", "lng", "addr_hash", "tag_id"))

  }


}
