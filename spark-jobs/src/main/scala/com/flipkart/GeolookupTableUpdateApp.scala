package com.flipkart

import java.io._
import java.util.Calendar

import com.datastax.spark.connector._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.flipkart.utils.HttpClient
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.lucene.util.GeoDistanceUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by vishal.srivastav on 23/11/17.
  */
object GeolookupTableUpdateApp {

  def filter_vector_delivery_verification_code_type_v1(vec: Vector[UDTValue]): Double = {
    var accuracy_level = 0.0;
    var delivery_verification_code_type = "INVALID_ID"
    for (i <- 0 to vec.length -1){

      if (vec(i).getString("key") == "delivery_verification_code_type"){
        delivery_verification_code_type = vec(i).getString("value")
      }
      if (vec(i).getString("key") == "accuracy_level"){
        accuracy_level = vec(i).getDouble("value")
      }

    }
    if (delivery_verification_code_type != "INVALID_ID"){
      return accuracy_level
    }
    else{
      return 0.0
    }
  }
  def filter_vector_delivery_verification_code_type(vec: Vector[UDTValue], del_type: String): Double = {
    var accuracy_level = 0.0;
    if (del_type == "DEL"){
      for (i <- 0 to vec.length -1){
        if (vec(i).getString("key") == "accuracy_level"){
          accuracy_level = vec(i).getDouble("value")
        }
      }
    }
    return accuracy_level
  }

  def get_best_latlng(vec: List[(String, Double, Double, Double)], url: String): (Double,Double) = {
    var length = vec.length;
    if (length < 4){
      return (vec(length-1)._2, vec(length-1)._3)
    }
    var body = "[";
    var latlng_list = List[String]();
    for (i <- 0 to vec.length-1){
      var lat = vec(i)._2.toString;
      var lng = vec(i)._3.toString;
      var tmp = "'" + lat + "," + lng + "'";
      latlng_list = tmp :: latlng_list;
    }
    body += latlng_list.mkString(",");
    body += "]";
    System.out.println(body);
    val headers = new java.util.HashMap[String, String]();
    var root: JsonNode = null
    headers.put("Content-Type", "application/json");
    try {
      val response = HttpClient.INSTANCE.executePost(url, body, headers);
      // System.out.println(response.getStatusCode)
      if (response.getStatusCode != 200) {
        return (vec(length-1)._2, vec(length-1)._3)
      } else {
        val mapper = new ObjectMapper()
        root = mapper.readTree(response.getResponseBody)
        System.out.println(root.get("lat").asDouble);
        return (root.get("lat").asDouble,root.get("lng").asDouble)
      }
    } catch {
      case e: Exception =>
        return (vec(length-1)._2, vec(length-1)._3)
    }
  }

  def vincenty_distance(lat_1:Double, lng_1:Double, lat_2:Double, lng_2:Double): Double ={

    if(lat_1 == 0 || lat_2 ==0)
      return 10000000
    val dist = GeoDistanceUtils.vincentyDistance(lng_1, lat_1, lng_2, lat_2)
    dist

  }

  def get_best_lat_lng(addr_hash: String, vec: List[(String, Double, Double, Double, Vector[UDTValue])]): (Double, Double) = {

    val length = vec.length

    Logger.log(this.getClass, INFO, BaseSLog(s"Calculating Best Lat Long for List of length : $length for addr_hash : $addr_hash"))

    if (length < 4 || length > 500){
      return (vec(length-1)._2, vec(length-1)._3)
    }
    var count_list = new mutable.ListBuffer[Integer]()

    for (i <- vec.indices){
      count_list+=0
    }
    var max_index = 0
    for (i <- vec.indices){
      for (j <- vec.indices) {
        if (vincenty_distance(vec(i)._2, vec(i)._3, vec(j)._2, vec(j)._3) < 300)
          count_list.update(i, count_list(i)+1)
      }
      if (count_list(max_index) < count_list(i))
        max_index = i
    }
    (vec(max_index)._2, vec(max_index)._3)
  }


  def deduplicateList(vec: List[(String, Double, Double, Double, Vector[UDTValue])]): List[(String, Double, Double, Double, Vector[UDTValue])] ={

    var locationSet = mutable.HashSet[(Double, Double, Double)]()

    var finalList = new mutable.ListBuffer[(String, Double, Double, Double, Vector[UDTValue])]()

    val initialLength = vec.length

    Logger.log(this.getClass, INFO, BaseSLog(s"Deduping List with Length : $initialLength"))

    for (i <- vec.indices){
      if(!locationSet.contains((vec(i)._2, vec(i)._3, vec(i)._4))){
        finalList+=vec(i)
        locationSet.+=((vec(i)._2, vec(i)._3, vec(i)._4))
      }
    }

    val finalLength = finalList.length

    Logger.log(this.getClass, INFO, BaseSLog(s"After Deduping Length : $finalLength"))

    if(finalLength > 100){
      finalList = finalList.takeRight(100)
    }

    finalList.toList
  }

  def getFilterDate(todayDate: Calendar): Calendar ={
    todayDate.add(Calendar.DATE, -7)
    return todayDate
  }

  def main(args: Array[String]) {
    val url = "http://10.51.194.206:5150/best-latlng-calculator"
    val filename = "/etc/fk-ekl-spark-jobs/spark-job-date.txt"
    var bufferDate = Calendar.getInstance()
    bufferDate.add(Calendar.DATE, -7)
    val simpleFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    var filterDate = simpleFormat.format(bufferDate.getTime)
    try {
      for (line <- Source.fromFile(filename).getLines) {
        filterDate = line
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Got an IOException!")
    }

    Logger.log(this.getClass, INFO, BaseSLog(s"Started from date: $filterDate"))
    val appName = "Geolookup Table Update"
    val sparkMaster = "spark://10.52.18.196:7077"
    val sparkMaxCores = "20"
    val sparkExecutorMemory = "15g"
    val cassandraHost = "10.50.243.81,10.48.242.126,10.50.19.80,10.50.99.43,10.51.227.19,10.50.67.89,10.51.83.74"
    val datacenter = "datacenter1"
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
      val rdd = sc.cassandraTable("compass", "geotag").select("addr_hash", "time", "lat", "lng", "attributes", "src", "type", "addr")
      // filtering invalid or not present delivery_verification_code_type
      //      val rdd2 = rdd.map(x => (x.getString("addr_hash"), (x.getString("time"), x.getDouble("lat"), x.getDouble("lng"), filter_vector_delivery_verification_code_type_v1(x.getList[UDTValue]("attributes")))))
      val rdd2 = rdd.map(x => (x.getString("addr_hash"), (x.getString("time"), x.getDouble("lat"), x.getDouble("lng"),
        filter_vector_delivery_verification_code_type(x.getList[UDTValue]("attributes"), x.getString("type")), x.getList[UDTValue]("addr"))))
      rdd.unpersist()
      // filtering on bad accuracy level
      val rdd3 = rdd2.filter(x => x._2._4 > 0 && x._2._4 < 200).filter(x=> x._2._1 > filterDate)
      // combining addr_hash
      // groupByKey takes a lot of memory
      val rdd4 = rdd3.combineByKey(
        // createCombiner: add first value to a list
        (x: (String, Double, Double, Double, Vector[UDTValue])) => List(x),
        // mergeValue: add new value to existing .l list
        (acc: List[(String, Double, Double, Double, Vector[UDTValue])], x) => x :: acc,
        // mergeCominber: combine the 2 lists
        (acc1: List[(String, Double, Double, Double, Vector[UDTValue])], acc2: List[(String, Double, Double, Double, Vector[UDTValue])]) => acc1 ::: acc2
        )

      val rdd1 = sc.cassandraTable("compass", "geotag_optimized_lookup").select("addr_hash", "attributes", "addr")
      val rdd1_2 = rdd1.map(x=>(x.getString("addr_hash"), x.getList[UDTValue]("attributes"), x.getList[UDTValue]("addr")))
      val rdd1_3 = rdd1_2.map(x=>(x._1, x._2.toList.map(y=>(y.getString("time"), y.getDouble("lat"), y.getDouble("lng"), y.getDouble("accuracy_level"), x._3))))
      rdd1.unpersist()
      //val rdd_union = rdd4.union(rdd1_3).reduceByKey((x,y) => x ::: y).map(x => (x._1, x._2.toList.sortBy(x => x._1))).filter(x => x._2.length > 0)

      val rdd_union = rdd4.union(rdd1_3).reduceByKey((x,y) => x ::: y).map(x => (x._1, x._2.sortBy(x => x._1))).filter(x => x._2.nonEmpty)
      val url = "http://10.51.194.206:5150/best-latlng-calculator"
      val filtered_deduped_rdd = rdd_union.map(x=> (x._1, deduplicateList(x._2)))
      val rdd1_4 = filtered_deduped_rdd.map(x => (x._1, get_best_lat_lng(x._1, x._2), x._2))
      val rdd1_5 = rdd1_4.map(x => (x._1, x._2._1, x._2._2, x._3.map(y=>(y._2,y._3,y._4,y._1)), "198", x._3.last._5))
      rdd1_5.saveToCassandra("compass", "geotag_optimized_lookup", SomeColumns("addr_hash", "cluster_lat", "cluster_lng", "attributes", "precision", "addr"))
      val file = new File(filename)
      val bw = new BufferedWriter(new FileWriter(file))
      val bufferDate = Calendar.getInstance()
      bw.write(simpleFormat.format(bufferDate.getTime))
      bw.close()
      //      val rdd = sc.cassandraTable("compass", "geotag").select("addr_hash", "time", "lat", "lng", "attributes", "src", "type")
//      // filtering invalid or not present delivery_verification_code_type
////      val rdd2 = rdd.map(x => (x.getString("addr_hash"), (x.getString("time"), x.getDouble("lat"), x.getDouble("lng"), filter_vector_delivery_verification_code_type_v1(x.getList[UDTValue]("attributes")))))
//      val rdd2 = rdd.map(x => (x.getString("addr_hash"), (x.getString("time"), x.getDouble("lat"), x.getDouble("lng"),
//        filter_vector_delivery_verification_code_type(x.getList[UDTValue]("attributes"), x.getString("type")))))
//      rdd.unpersist()
//      // filtering on bad accuracy level
//      val rdd3 = rdd2.filter(x => x._2._4 > 0 && x._2._4 < 200)
//      // combining addr_hash
//      // groupByKey takes a lot of memory
//      val rdd4 = rdd3.combineByKey(
//        // createCombiner: add first value to a list
//        (x: (String, Double, Double, Double)) => List(x),
//        // mergeValue: add new value to existing .l list
//        (acc: List[(String, Double, Double, Double)], x) => x :: acc,
//        // mergeCominber: combine the 2 lists
//        (acc1: List[(String, Double, Double, Double)], acc2: List[(String, Double, Double, Double)]) => acc1 ::: acc2
//      )
//      Logger.log(this.getClass, INFO, BaseSLog(s"combine by key completed"))
//
//      val rdd5 = rdd4.map(x => (x._1, x._2.toList.sortBy(x => x._1)))
//      val rdd7 = rdd5.map(x => (x._1, x._2(x._2.size - 1)._1, x._2))
//      Logger.log(this.getClass, INFO, BaseSLog(s"Filtering for date: $lastDate"))
//      val rdd7_cnt = rdd7.count()
//      Logger.log(this.getClass, INFO, BaseSLog(s"count for rdd7: $rdd7_cnt"))
//      val rdd8 = rdd7.map(x => (x._1, get_best_latlng(x._3, url), x._3))
//      val rdd8_format = rdd8.map(x => (x._1, x._2._1, x._2._2, x._3.map(y=>(y._2,y._3,y._4,y._1)), "198"))
//      Logger.log(this.getClass, INFO, BaseSLog(s"starting saving to cassandra"))
//      rdd8_format.saveToCassandra("compass", "geotag_optimized_lookup", SomeColumns("addr_hash", "cluster_lat", "cluster_lng", "attributes", "precision"))
//      Logger.log(this.getClass, INFO, BaseSLog(s"completed saving to cassandra"))

//      val bufferDate = Calendar.getInstance()
//      bufferDate.add(Calendar.DATE, -2)
//      val simpleFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
//      val newDate = simpleFormat.format(bufferDate.getTime)
//      client.setData().forPath(zNodePath, newDate.getBytes())
//      client.close()
    }
    catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing GeolookupTableUpdateApp: " + e.getMessage, e))
        throw e
    }
    finally{
      sys.ShutdownHookThread {
        Logger.log(this.getClass, INFO, BaseSLog(s"Gracefully stopping GeolookupTableUpdateApp"))
        sc.stop()
        Logger.log(this.getClass, ERROR, BaseSLog(s"GeolookupTableUpdateApp stopped"))
//        client.close()
      }
    }

  }

}