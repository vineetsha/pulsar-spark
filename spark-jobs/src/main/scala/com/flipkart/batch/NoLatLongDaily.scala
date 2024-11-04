package com.flipkart.batch

/**
 * Created by sharma.varun on 31/07/15.
 */
import java.io.{BufferedWriter, File, FileWriter}

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

object NoLatLongDaily {
  type TupleType = (Double,Double,Double)

  def countBad(item:Traversable[TupleType]) : Int = {
    item.toList.count(x => {
      x._1 == 0 && x._2 == 0
    })
  }

  def calculate(rdd:CassandraTableScanRDD[CassandraRow], date_today:String): RDD[(String, Int)] = {
    val data3 = rdd
      .filter(x => x.getString("date") == date_today)
      .map(x =>
      (
        x.getString("device_id"),
        (x.getDouble("lat"),
          x.getDouble("lng"),
          x.getDouble("alt")
          ))
      )
    val data4 = data3.groupByKey()
    val data5 = data4.map(x => (x._1, countBad(x._2))).filter(x => x._2 != 0L)
    data5
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
      .setAppName("NoLatLongCountReport")
      .setMaster(sparkMaster)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.executor.extraJavaOptions", """-Dlog4j.configuration=file:/usr/share/spark/conf/log4j-compass-streaming-executor.properties -Dcom.sun.management.jmxremote.port=21837 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.net.preferIPv4Stack=true""")
      .set("spark.logConf", "true")
    )

    val rdd = sc.cassandraTable("compass", "geotag").select("device_id", "date", "alt", "lat", "lng", "tag_id")
    val rdd2 = sc.cassandraTable("compass", "event_history").select("device_id", "date", "alt", "lat", "lng")

    val data4: RDD[(String, Int)] = calculate(rdd, date_today)
    val data5 = data4.collect()
    val data6 = data5.map(x => x._2).sum
    val data7: RDD[(String, Int)] = calculate(rdd2, date_today)
    val data8 = data7.map(x => x._2).sum

    val out = new BufferedWriter(new FileWriter("no_lat_long_count_geotag.csv"))
    val writer = new CSVWriter(out)
    writer.writeNext(Array("GeoTag Total->",data6.toString))
    writer.writeNext(Array("Event Total->",data8.toString))
    writer.writeNext(("device_id" ,"0,0 lat/lng count").productIterator.toArray.map(x => x.toString))

    for (elem <- data5) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }
    writer.close()

    if (Utility.getEnvironmentVariable("APP_ENV").getOrElse("default").equals("nm")) {
      Logger.log(this.getClass, INFO, BaseSLog(s"Sending mail"))
      send a new Mail(
        from = ("sharma.varun@flipkart.com", "Varun Sharma"),
        to = Seq("lm-geo-dev@flipkart.com","scm-mobile-vas-platform@flipkart.com"),
        cc = Seq("shivram.k@flipkart.com"),
        subject = "No Lat/Lng Report",
        message = "PFA the report....",
        attachment = Option(new File("no_lat_long_count_geotag.csv")),
        hostName = "10.33.102.104",
        username = "lm.geo",
        password = password
      )
    }
  }

}
