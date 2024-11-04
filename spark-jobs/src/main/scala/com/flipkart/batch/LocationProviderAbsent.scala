package com.flipkart.batch

import java.io.{FileWriter, BufferedWriter}

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by sharma.varun on 26/10/15.
 */
object LocationProviderAbsent {
  val sc = new SparkContext(new SparkConf().setAppName("AccuracyReport"))
  val rdd = sc.cassandraTable("compass", "geotag").select("device_id", "date", "src", "type", "time", "addr", "addr_full", "alt", "attributes", "lat", "lng", "tag_id")
  val data3 = rdd.filter(x =>{
    !x.getList[UDTValue]("attributes").map(x=>x.getString("key")).contains("location_provider")
  })

  val data4 = rdd.filter(x=> {
    x.getDouble("lat") == 0.0 && x.getDouble("lng") == 0.0
  })

  val data5 = rdd.filter(x=> {
    x.getDouble("lat") == 0.0 && x.getDouble("lng") == 0.0 && !x.getList[UDTValue]("attributes").map(x=>x.getString("key")).contains("location_provider")
  }).map(x=>(x.getString("device_id"),x.getString("date")))
    .groupByKey()
    .map(x=>(x._1,x._2.toList.size))
  val data6 = data5.collect()

  val out = new BufferedWriter(new FileWriter("faulty_devices.csv"))
  val writer = new CSVWriter(out)
  for (elem <- data6) {
    writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
  }
  writer.close()
}
