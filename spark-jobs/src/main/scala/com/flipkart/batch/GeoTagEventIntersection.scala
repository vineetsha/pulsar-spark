package com.flipkart.batch

import java.io.{BufferedWriter, FileWriter}

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector._
import com.flipkart.config.Config
import com.flipkart.utils.Mail.{Mail, send}
import com.flipkart.utils.Utility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by sharma.varun on 23/10/15.
 */
object GeoTagEventIntersection {
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
      .setAppName("EventGeoTagConsistencyChecker")
      .setMaster(sparkMaster)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming-executor.properties -Dcom.sun.management.jmxremote.port=29497 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
    )
    val rdd_geotag = sc.cassandraTable("compass", "geotag").select("device_id", "date")
    val data_geotag = rdd_geotag.map(x => (x.getString("device_id"), x.getString("date"))).filter(x => x._2 == date_today)
    /*
    val rdd_event = sc.cassandraTable("compass", "event").select("device_id")
    val data_event = rdd_event.map(x=>x.getString("device_id")).collect()

    val faulty_devices=data_geotag.filter(x=>{
      !data_event.contains(x._1)
    })
    val faulty_device_count = faulty_devices.countByKey()
    val out = new BufferedWriter(new FileWriter("no_event_just_geotag.csv"))
    val writer = new CSVWriter(out)
    writer.writeNext(Array("no_event_device_id" ,"geotag_count"))
    for (elem <- faulty_device_count) {
      writer.writeNext(Array(elem._1, elem._2.toString))
    }
    writer.close()
    */

    val rdd_event_history = sc.cassandraTable("compass", "event_history").select("device_id", "date")
    val data_event_history = rdd_event_history.map(x => (x.getString("device_id"), x.getString("date"))).filter(x => x._2 == date_today)
    val event_history_count = data_event_history.countByKey()
    val geotag_count = data_geotag.countByKey()
    val device_id_set = event_history_count.keySet.union(geotag_count.keySet)
    val out2 = new BufferedWriter(new FileWriter("event_geotag_ratio.csv"))
    val writer2 = new CSVWriter(out2)
    writer2.writeNext(Array("device_id", "Ratio:event_count/geotag_count", "event_count", "geotag_count"))
    var result = Map[String, (Double, Long, Long)]()
    for (elem <- device_id_set) {
      var ratio = 0.0
      var event_history_size = Long.MaxValue
      var geotag_size = Long.MaxValue
      if (!geotag_count.contains(elem)) {
        ratio = Double.MaxValue
        geotag_size = 0L
        event_history_size = event_history_count.get(elem).get
      }
      else if (!event_history_count.contains(elem)) {
        ratio = 0.0
        event_history_size = 0L
        geotag_size = geotag_count.get(elem).get
      }
      else {
        geotag_size = geotag_count.get(elem).get
        event_history_size = event_history_count.get(elem).get
        ratio = event_history_size.toDouble / geotag_size.toDouble
      }
      if (ratio <= 2.0) {
        result += elem ->(ratio, event_history_size, geotag_size)
      }
    }
    //  result.keySet.diff(faulty_device_count.keySet)
    for (elem <- result) {
      writer2.writeNext(Array(elem._1) ++ elem._2.productIterator.toArray.map(x => x.toString))
    }
    writer2.close()
    if (Utility.getEnvironmentVariable("APP_ENV").getOrElse("default").equals("nm")) {
      Logger.log(this.getClass, INFO, BaseSLog(s"Sending mail"))
      send a new Mail(
        from = ("sharma.varun@flipkart.com", "Varun Sharma"),
        to = Seq("lm-geo-dev@flipkart.com","scm-mobile-vas-platform@flipkart.com"),
        cc = Seq("shivram.k@flipkart.com"),
        subject = "GeoTag Event Mismatch Report",
        message = "PFA the report....",
        attachment = Option(new java.io.File("event_geotag_ratio.csv")),
        hostName = "10.33.102.104",
        username = "lm.geo",
        password = password
      )
    }
  }
}
