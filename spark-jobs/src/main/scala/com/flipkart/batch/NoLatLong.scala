package com.flipkart.batch

/**
 * Created by sharma.varun on 31/07/15.
 */
import java.io.{BufferedWriter, FileWriter}
import java.text.SimpleDateFormat

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
object NoLatLong {
  type TupleType = (String,Double,Double,Double)

  def parseDate(value: String) = {
    try {
      Some(new SimpleDateFormat("yyyy-MM-dd").parse(value))
    } catch {
      case e: Exception => None
    }
  }

  def countBad(item:Traversable[TupleType]) : Int = {
    item.toList.count(x => {
      x._2 == 0 && x._3 == 0
    })
  }
  def sortCountByDate(device_id:String, item:Traversable[(String,Int)]) : List[String] = {
    //
    val dateRange = List("2015-07-20","2015-07-21" ,"2015-07-22","2015-07-23","2015-07-24","2015-07-25","2015-07-26","2015-07-27","2015-07-28","2015-07-29","2015-07-30")
    val countArray = new ListBuffer[String]()
    countArray += device_id
    for (date <- dateRange) {
      val data = item.toList.filter(x => x._1 == date)
      if (data.isEmpty){
        countArray += "No Data"
      }
      else{
        countArray += data.head._2.toString
      }

    }
    countArray.toList
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("AccuracyReport"))
    val rdd = sc.cassandraTable("compass", "geotag").select("device_id", "date", "alt", "lat", "lng", "tag_id")

    val data3 = rdd
      .filter(x=>{
      parseDate(x.getString("date")).get.after(parseDate("2015-07-19").get) && parseDate(x.getString("date")).get.before(parseDate("2015-07-31").get)
    })
      .map(x =>
      (
        (x.getString("device_id"), x.getString("date")),
        (x.getString("tag_id"),
          x.getDouble("lat"),
          x.getDouble("lng"),
          x.getDouble("alt")
          ))
      )
    val data4 = data3.groupByKey()
    val data5=data4.map(x=> (x._1._1, (x._1._2, countBad(x._2))))
    val data6=data5.groupByKey().map(x=> sortCountByDate(x._1, x._2)).collect()
    val out = new BufferedWriter(new FileWriter("no_lat_long_data_20July2015_30July2015.csv"))
    val writer = new CSVWriter(out)
    writer.writeNext(("device_id" ,"2015-07-20","2015-07-21" ,"2015-07-22","2015-07-23","2015-07-24","2015-07-25","2015-07-26","2015-07-27","2015-07-28","2015-07-29","2015-07-30").productIterator.toArray.map(x => x.toString))
    for (elem <- data6) {
      writer.writeNext(elem.toArray.map(x => x.toString))
    }
    writer.close()
  }

}
