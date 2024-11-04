package com.flipkart.batch

import java.io.{BufferedWriter, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector._
import com.flipkart.utils.Mail.{Mail, send}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}


/**
 * Created by shivang.b on 12/30/15.
 */
object GenericEventCount {

  def main (args: Array[String]) {

    var password: String = null
    if (args.length < 1) {
      System.err.println("Usage: GenericEventCount <smtp:password>")
      System.exit(1)
    }
    else {
      password = args(0)
    }

    val cal = Calendar.getInstance();
    val format = new SimpleDateFormat("y-M-d")
    cal.add(Calendar.DATE, -1)
    val yesterday = format.format(cal.getTime())
    val sc = new SparkContext(new SparkConf().setAppName("InfoTrackMetrics"))
    val rdd = sc.cassandraTable("compass", "event_history")
    val infotrackfm = rdd.filter(x => x.getString("src") == "INFOTRACK_FirstMile")
    val infotrackdayfm = infotrackfm.filter(x => x.getString("date") == yesterday)

    val infotracklm = rdd.filter(x => x.getString("src") == "INFOTRACK_FirstMile")
    val infotrackdaylm = infotracklm.filter(x => x.getString("date") == yesterday)

    val novire = rdd.filter(x => x.getString("src") == "Novire_Transport")
    val novireday = novire.filter(x => x.getString("date") == yesterday)

    val out = new BufferedWriter(new FileWriter("genericEventAPI_counts.csv"))
    val writer = new CSVWriter(out);
    writer.writeNext(Array("Date:", yesterday))
    writer.writeNext(Array("Vendor", "Unique payloads"))
    writer.writeNext(Array("INFOTRACK_FirstMile", infotrackdayfm.count.toString))
    writer.writeNext(Array("INFOTRACK_FirstMile", infotrackdaylm.count.toString))
    writer.writeNext(Array("Novire_Transport", novireday.count.toString))

    writer.close()

    Logger.log(this.getClass, INFO, BaseSLog(s"Sending mail"))
    send a new Mail(
      from = ("shivang.b@flipkart.com", "Shivang Bhatt"),
      to = Seq("lm-geo-dev@flipkart.com"),
      subject = "Generic API payload count",
      message = "PFA the vendor vise payload count report",
      attachment = Option(new java.io.File("GenericEventAPI_counts.csv")),
      hostName = "10.33.102.104",
      username = "lm.geo",
      password = password
    )

  }

}
