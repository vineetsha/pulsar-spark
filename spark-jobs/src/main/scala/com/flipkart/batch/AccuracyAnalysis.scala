package com.flipkart.batch

import java.io.{File, FileWriter, BufferedWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util._

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.flipkart.config.Config
import com.flipkart.utils.Mail.{Mail, send}
import com.flipkart.utils.Utility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime


object AccuracyAnalysis {
  def percentile[T](item:Traversable[T], p:Double)(implicit n:Numeric[T]) = {
    var sortedList = item.toList.sorted
    var idx = p * sortedList.size - 1
    sortedList(idx.toInt)
  }
  def mean[T](item:Traversable[T])(implicit n:Numeric[T]) = {
    n.toDouble(item.sum) / item.size.toDouble
  }
  def variance[T](items:Traversable[T])(implicit n:Numeric[T]) : Double = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total,item)=>{
      val itemDbl = n.toDouble(item)
      val square = math.pow(itemDbl - itemMean,2)
      total + square
    })
    sumOfSquares / count.toDouble
  }
  def stddev[T](items:Traversable[T])(implicit n:Numeric[T]) : Double = {
    math.sqrt(variance(items))
  }

  def old_main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("AccuracyReport"))
    var rdd = sc.cassandraTable[(String, String, Date, Double,Integer)]("compass", "location_history").select("device_id", "date", "time", "acc","signal")

    //var rdd2 = rdd.filter(x=> (x._2=="2015-03-02"))
    var scalaArray =  rdd.map(x=>(x._1,x._4)).groupByKey().map(x=>(x._1,x._2.size,percentile(x._2, 0.5),percentile(x._2, 0.9),mean(x._2),stddev(x._2))).collect()
    var output = "IMEI,Samples,Accuracy(50 percentile),Accuracy(90 percentile),Accuracy(mean),Accuracy(Std. Dev.)\n"
    for(elem <- scalaArray ) {
      output = output + elem._1 + "," + elem._2 + "," + elem._3 + "," + elem._4 + "," + elem._5 + "," + elem._6 + "\n"
    }
    //var scalaString = scalaArray.mkString("\n")
    Files.write(Paths.get("accuracy.txt"), output .getBytes(StandardCharsets.UTF_8))

  }

  def data_array(rdd: CassandraTableScanRDD[CassandraRow]): RDD[(String, String,Double, Double, Double, String)] = {
    val data = rdd.filter(x => x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "accuracy_level") != Nil)
      .map(x => {
      (
        x.getString("device_id"),
        x.getString("date"),
        x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "accuracy_level").head.getString("value").toDouble,
        x.getDouble("lat"),
        x.getDouble("lng"),
        x.getString("src")
        )
    })
    data
  }
  def countSplit(item:Traversable[Double], low:Int, high:Int) : Int = {
    item.toList.count(x => {x>low && x<=high })
  }

  type PercentileTuple = (String,String,Int,Double,Double,Double,Double,Double,Double,Double)
  type CountSplitTuple = (String,String,Int,Int,Int,Int,Int,Int)

  def buildPercentileTuple(dataType:String, x :(String,Iterable[Double])):PercentileTuple ={
    return (dataType,x._1,x._2.size,percentile(x._2, 0.5),percentile(x._2, 0.75),percentile(x._2, 0.90),percentile(x._2, 0.95),percentile(x._2, 0.99),mean(x._2),stddev(x._2))
  }

  def buildCountSplitTuple(dataType:String, x :(String,Iterable[Double])):CountSplitTuple={
    return (dataType,x._1,x._2.size,countSplit(x._2,Int.MinValue,0),countSplit(x._2,0,50),countSplit(x._2,50,100),countSplit(x._2,100,500),countSplit(x._2,500,Int.MaxValue))
  }

  def main(args: Array[String]): Unit ={
    var password: String = null
    var date_today: String = null
    if (args.length < 2) {
      System.err.println("Usage: AccuracyAnalysis <smtp:password> <date>")
      System.exit(1)
    }
    else {
      password = args(0)
      date_today = args(1)
    }

    var from = new DateTime(date_today)
    var format = DateTimeFormat.forPattern("yyyy-MM-dd")
    var last7days = for (f<- 0 to 7) yield format.print(from.minusDays(f))

    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.84.56.172")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.84.56.172:7077,10.84.56.173:7077,10.84.56.197:7077")

    val sc = new SparkContext(new SparkConf()
      .setAppName("AccuracyAnalysisBatch")
      .setMaster(sparkMaster)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming-executor.properties -Dcom.sun.management.jmxremote.port=21837 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
    )

    val geoTagRdd = sc.cassandraTable("compass", "geotag").select("device_id", "date","time", "attributes", "lat", "lng", "src")
    val eventHistoryRdd = sc.cassandraTable("compass", "event_history").select("device_id","date","time", "attributes", "lat", "lng", "src")

    val geoTagAccuracyData = data_array(geoTagRdd)
      .filter(x=>(last7days.contains(x._2)))
      .map(x=>(x._2,x._3))
      .groupByKey()
      .map(x=>buildPercentileTuple("geotag",x))
      .collect()

    val geotagAccuracySplit = data_array(geoTagRdd)
      .filter(x=>(last7days.contains(x._2)))
      .map(x=>(x._2,x._3))
      .groupByKey()
      .map(x=>buildCountSplitTuple("geotag",x))
      .collect()

    val eventAccuracyData = data_array(eventHistoryRdd)
      .filter(x=>(last7days.contains(x._2)))
      .map(x=>(x._2,x._3))
      .groupByKey()
      .map(x=>buildPercentileTuple("event",x))
      .collect()

    val eventAccuracySplit = data_array(eventHistoryRdd)
      .filter(x=>(last7days.contains(x._2)))
      .map(x=>(x._2,x._3))
      .groupByKey()
      .map(x=>buildCountSplitTuple("event",x))
      .collect()


    val out = new BufferedWriter(new FileWriter("accuracy.csv"))
    val writer = new CSVWriter(out)

    writer.writeNext(("type","date","Samples","Accuracy(50 percentile)","Accuracy(75 percentile)","Accuracy(90 percentile)","Accuracy(95 percentile)","Accuracy(99 percentile)","Accuracy(mean)","Accuracy(Std. Dev.)").productIterator.toArray.map(x => x.toString))
    for (elem <- geoTagAccuracyData) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }
    for (elem <- eventAccuracyData) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }

    writer.writeNext(("type","date","Samples","(<0)","(>0-50)","(>50-100)","(>100-500)","(>500)").productIterator.toArray.map(x => x.toString))
    for (elem <- geotagAccuracySplit) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }
    for (elem <- eventAccuracySplit) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }
    writer.close()

    if (Utility.getEnvironmentVariable("APP_ENV").getOrElse("default").equals("nm")) {
      Logger.log(this.getClass, INFO, BaseSLog(s"Sending mail"))
      send a new Mail(
        from = ("sourav.roy@flipkart.com", "Sourav"),
        to = Seq("flip-dev@flipkart.com","scm-mobile-vas-platform@flipkart.com"),
        cc = Seq("sourav.roy@flipkart.com"),
        subject = "Data Accuracy Report",
        message = "PFA the report....",
        attachment = Option(new File("accuracy.csv")),
        hostName = "10.33.102.104",
        username = "lm.geo",
        password = password
      )
    }
  }
}
