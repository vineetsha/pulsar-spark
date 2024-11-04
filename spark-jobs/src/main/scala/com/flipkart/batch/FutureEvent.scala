package com.flipkart.batch


import com.flipkart.config.Config
import com.flipkart.utils.Mail.{Mail, send}
import com.flipkart.utils.Utility
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter

/**
 * Created by sharma.varun on 22/12/15.
 */
object FutureEvent {
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

    val rdd_geotag = sc.cassandraTable("compass", "geotag").select("device_id", "date").map(x => (x.getString("device_id"), x.getString("date"))).filter(x => x._2 > "2015-12-18")
    val rdd_event_history = sc.cassandraTable("compass", "event_history").select("device_id", "date").map(x => (x.getString("device_id"), x.getString("date"))).filter(x => x._2 > "2015-12-18")

    val data = rdd_geotag.collect()
    val data_event= rdd_event_history.collect()
    val data_geotag_grouped=data.groupBy(x=>x._1).map(x=>(x._1,x._2.length))
    val data_event_grouped=data_event.groupBy(x=>x._1).map(x=>(x._1,x._2.length))

    val out = new BufferedWriter(new FileWriter("future_event_count.csv"))
    val writer = new CSVWriter(out)
    for (elem <- data_event_grouped) {
      writer.writeNext(Array(elem._1, elem._2.toString))
    }
    writer.close()

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
