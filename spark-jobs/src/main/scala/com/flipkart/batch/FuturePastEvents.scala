package com.flipkart.batch

import java.io.{File, BufferedWriter, FileWriter}
import java.text.SimpleDateFormat

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.{UDTValue, _}
import com.flipkart.config.Config
import com.flipkart.utils.Mail.{Mail, send}
import com.flipkart.utils.{TimeFormatTransformation, Utility}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by sharma.varun on 26/10/15.
 */
object FuturePastEvents {

  def formatTime(diff:Long):(Long, String) = {

    val diffSeconds:Long = diff / 1000 % 60
    val diffMinutes:Long = diff / (60 * 1000) % 60
    val diffHours:Long = diff / (60 * 60 * 1000) % 24
    val diffDays:Long = diff / (24 * 60 * 60 * 1000)
    // return tuple of total seconds and formatted total time
    (diff / 1000, diffDays + " days " + diffHours + " hours " + diffMinutes + " minutes " + diffSeconds + " seconds.")
  }

  def filterByFuturePast(time:Long):Boolean={
    // if date is older than 1 day(24*60*60 seconds) or in future of 1 second
    time > 0L || time < -(24L * 60L * 60L)
  }

  def data_array(_type:String, date_today: String, rdd: CassandraTableScanRDD[CassandraRow]): RDD[(String, Long, String, String, String, String, Double, Double)] = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val data = rdd.filter(x => x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "received_at") != Nil)
      .map(x => {
      (
        x.getString("time"),
        x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "received_at").head.getString("value"),
        x.getString("device_id"),
        x.getDouble("lat"),
        x.getDouble("lng"),
        x.getString("src")
        )
    })
      .map(x => (formatTime(format.parse(x._1).getTime - format.parse(x._2).getTime), x._1, x._2, x._3, x._4, x._5, x._6))
      .filter(x => TimeFormatTransformation.convertTime(x._3, "yyyy-MM-dd HH:mm:ssZ", "yyyy-MM-dd", "IST").equals(date_today))
      .filter(x => x._7 == "fsd")
      .map(x => (_type, x._1._1, x._1._2, x._2, x._3, x._4, x._5, x._6))
    data
  }

  def main(args: Array[String]) {
    var password: String = null
    var date_today: String = null

    if (args.length < 2) {
      System.err.println("Usage: KafkaCassandraConsisencyChecker <smtp:password> <date>")
      System.exit(1)
    }
    else {
      password = args(0)
      date_today = args(1)
    }
    lazy val cassandraHost = Config.getProperty("spark.StreamingApp.cassandraHost", "10.75.123.103")
    lazy val sparkMaster = Config.getProperty("spark.StreamingApp.sparkMaster", "spark://10.75.123.103:7077,10.75.123.104:7077,10.75.123.105:7077")
    val sc = new SparkContext(new SparkConf()
      .setAppName("FuturePastEvents")
      .setMaster(sparkMaster)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming-executor.properties -Dcom.sun.management.jmxremote.port=29997 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
    )
    val rdd = sc.cassandraTable("compass", "geotag").select("device_id", "time", "attributes", "lat", "lng", "src")
    val rdd2 = sc.cassandraTable("compass", "event_history").select("device_id", "time", "attributes", "lat", "lng", "src")

    val dataGeoTagTotalCount:Long = data_array("GeoTag", date_today, rdd).count()
    val dataGeoTagFiltered: Array[(String, Long, String, String, String, String, Double, Double)] =
      data_array("GeoTag", date_today, rdd).filter(x => filterByFuturePast(x._2)).collect()

    val dataEventTotalCount:Long = data_array("Event", date_today, rdd2).count()
    val dataEventFilteredCount: Long = data_array("Event", date_today, rdd2).filter(x => filterByFuturePast(x._2)).count()

    val out = new BufferedWriter(new FileWriter("future-past-dates.csv"))
    val writer = new CSVWriter(out)
    writer.writeNext(Array("Event Count", dataEventFilteredCount.toString, "Out of", dataEventTotalCount.toString, "percent", ((dataEventFilteredCount.toDouble*100)/dataEventTotalCount.toDouble).toString))
    writer.writeNext(Array("GeoTag Count", dataGeoTagFiltered.length.toString, "Out of", dataGeoTagTotalCount.toString, "percent", ((dataGeoTagFiltered.length.toDouble*100)/dataGeoTagTotalCount.toDouble).toString))
    writer.writeNext(Array("Geotag Samples--->"))
    writer.writeNext(Array("name","diff in seconds", "total diff", "device time", "received at", "device_id", "lat", "lng"))

    for (elem <- dataGeoTagFiltered) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }
    writer.close()

    if (Utility.getEnvironmentVariable("APP_ENV").getOrElse("default").equals("nm")) {
      Logger.log(this.getClass, INFO, BaseSLog(s"Sending mail"))
      send a new Mail(
        from = ("sharma.varun@flipkart.com", "Varun Sharma"),
        to = Seq("lm-geo-dev@flipkart.com","scm-mobile-vas-platform@flipkart.com"),
        cc = Seq("shivram.k@flipkart.com"),
        subject = "Future and Past GeoTag/Events",
        message = "PFA the report....",
        attachment = Option(new File("future-past-dates.csv")),
        hostName = "10.33.102.104",
        username = "lm.geo",
        password = password
      )
    }
  }
}
