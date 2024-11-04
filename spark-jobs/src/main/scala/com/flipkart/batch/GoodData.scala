package com.flipkart.batch

/**
 * Created by sharma.varun on 31/07/15.
 */
import java.io.{BufferedWriter, FileWriter}
import java.util.{Calendar, Date}

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.flipkart.config.Config
import com.flipkart.utils.HttpClient
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object GoodData {
  type TupleType = (String,Double,Double,Double,String,String,Date,String,String,String,String,String,String,String,String)
  var reverse_geocode_url = "http://fms-sage-app.vip.nm.flipkart.com:8888/reverse_geocode"

  def filterBad(item:Traversable[TupleType]) : Traversable[TupleType] = {
    val sortedList = item.toList.sortBy(x => x._7)
    //filter by consecutive deliveries within 120 seconds
    var x = ListBuffer[TupleType]()
    for (i<- 1 to sortedList.size -1){
      if (!(((sortedList(i)._7.getTime - sortedList(i-1)._7.getTime)/1000) <= 120 && sortedList(i)._8.compareTo(sortedList(i-1)._8) != 0)){
        if (i==1) x+=sortedList(i-1)
        x += sortedList(i)
      }
    }
    val filteredList = x.toList
    val finalList = filteredList.filter(x => x._2.toDouble.!=(0.0) && x._3.toDouble.!=(0.0)  && x._15.toDouble.<(50.0))
    //keep data between 7 in morning to 8 in evening
    val calendar = Calendar.getInstance();
    finalList.filter(x => {
      calendar.setTime(x._7)
      val hour = calendar.get(Calendar.HOUR_OF_DAY)
      hour >= 7 && hour <= 20
    })
    .filter(x=>x._6.equals("DEL"))
    //.filter(x=>(x._11.equalsIgnoreCase("bangalore") || x._11.equalsIgnoreCase("BENGALURU")))
  }

  def call_reverse_geocode(lat:Double, lng:Double): JsonNode ={
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    val reverse_geoCoderParams = new java.util.HashMap[String,String]()
    reverse_geoCoderParams.put("point", lat.toString+","+lng.toString)
    val response = HttpClient.INSTANCE.executeGet(reverse_geocode_url,reverse_geoCoderParams, headers)

    if (response.getStatusCode != 200) {
      Logger.log(this.getClass, ERROR, BaseSLog("[geocode] Error, Returned status code " + response.getStatusCode))
      null
    }else {
      val mapper = new ObjectMapper()
      val root = mapper.readTree(response.getResponseBody)
      (root)
    }
  }
  def reverse_geocode(lat:Double, lng:Double): String = {
    try {
      var resp = call_reverse_geocode(lat, lng)
      if (resp != null && resp.get("results") != null && resp.get("results").size() > 0) {
        return resp.get("results").get(0).toString
      } else {
        return ""
      }
    } catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("[geocode] Error, Returned status code " + e))
        return ""
    }finally {
      return ""
    }
  }

  def dump_geotag_good(rdd: CassandraTableScanRDD[CassandraRow]) {
    val data2 = rdd.filter(x => x.getList[UDTValue]("attributes")
      .filter(x =>
      x.getString("key") == "delivery_verification_code_type"
        && (x.getString("value") == "ORDER_ID" || x.getString("value") == "TRACKING_ID"))
      != Nil)
    //.filter(x=>x.getString("date")=="2015-07-31")
    val data3 = data2.map(x =>
      (
        (x.getString("device_id"), x.getString("date")),
        (x.getString("tag_id"),
          x.getDouble("lat"),
          x.getDouble("lng"),
          x.getDouble("alt"),
          x.getString("src"),
          x.getString("type"),
          x.getDate("time"),
          x.getString("addr_full"),
          x.getList[UDTValue]("addr").filter(x => x.getString("key") == "addr1")(0).getString("value"),
          x.getList[UDTValue]("addr").filter(x => x.getString("key") == "addr2")(0).getString("value"),
          x.getList[UDTValue]("addr").filter(x => x.getString("key") == "city")(0).getString("value"),
          x.getList[UDTValue]("addr").filter(x => x.getString("key") == "state")(0).getString("value"),
          x.getList[UDTValue]("addr").filter(x => x.getString("key") == "pincode")(0).getString("value"),
          x.getList[UDTValue]("addr").filter(x => x.getString("key") == "country")(0).getString("value"),
          x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "accuracy_level")(0).getString("value")
          ))
    )
    val data4 = data3.groupByKey()
    //    data4.take(2)
    val data5 = data4.map(x => (x._1, filterBad(x._2)))
    /*
      val data4data= data4.collect
      val data5data = data5.collect

      for (device <- data4data.map(x=> x._1._1)){
        val diff = data4data.filter(x=> x._1._1==device)(0)._2.toList.size - data5data.filter(x=> x._1._1==device)(0)._2.toList.size
        println(device + ":" + diff)
      }

      var data4data_list = data4data.filter(x=> x._1._1=="865520020136512")(0)._2.toList
      var data5data_list = data5data.filter(x=> x._1._1=="865520020136512")(0)._2.toList

      data5data_list.map(x=> println(x))

      for (i <- 0 to data4data.size-1){
        val diff =data5data(i)._2.toList.size - data4data(i)._2.toList.size
        println(i+":" + diff)
      }
  */
    val data6 = data5.flatMap({ case (key, groupValues) => groupValues
      .map { value =>
      (key._1,
        value._1, value._2, value._3, value._4, value._5,
        value._6, value._7.toString, value._8, value._9, value._10,
        value._11, value._12, value._13, value._14, value._15, reverse_geocode(value._2,value._3))
    }
    }).collect()


    val out = new BufferedWriter(new FileWriter("filtered_data_till_2015-08-01_bangalore.csv"));
    val writer = new CSVWriter(out);
    writer.writeNext(Array("device_id", "tag_id", "lat", "lng", "alt", "src", "type", "time", "addr_full", "addr1", "addr2", "city", "state", "pincode", "country", "accuracy_level","reverse_geocode"))
    for (elem <- data6) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }
    writer.close()
  }
  
  def main(args: Array[String]) {
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.84.56.172")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.84.56.172:7077,10.84.56.173:7077,10.84.56.197:7077")
    val sc = new SparkContext(new SparkConf()
      .setAppName("FetchFilteredGeoTags")
      .setMaster(sparkMaster)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming-executor.properties -Dcom.sun.management.jmxremote.port=21837 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
    )

    val rdd = sc.cassandraTable("compass", "geotag").select("device_id", "date", "src", "type", "time", "addr", "addr_full", "alt", "attributes", "lat", "lng", "tag_id")
//    val data = rdd.filter(x => x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "verified_by" && (x.getString("value") == "barcode" || x.getString("value") == "pin")) != Nil)
    dump_geotag_good(rdd)
  }

}
