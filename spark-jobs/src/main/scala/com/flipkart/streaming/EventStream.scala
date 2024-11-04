package com.flipkart.streaming


import java.text.SimpleDateFormat

import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.{SomeColumns, UDTValue}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ListBuffer


/**
 * Created by sharma.varun on 11/12/14.
 */
object EventStream extends Logging {

  var TOPIC_NAME = "event_feed"

  var CASSANDRA_KEY_SPACE = "compass"

  var EVENT_TABLE = "event"

  var EVENT_HISTORY_TABLE = "event_history"

  def convertAttributes(attributes:Map[String,String]):
  List[UDTValue] ={
    var list = new ListBuffer[UDTValue]
    for ((key, value) <- attributes) {
      var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
      list += udtValue
    }
    return list.toList
  }

  def run(ssc: StreamingContext, zookeeperQuorum: String) {

    val stream = KafkaUtils.createStream(ssc, zookeeperQuorum, TOPIC_NAME, Map(TOPIC_NAME -> 3))
    logInfo("Reading stream from topic: " + TOPIC_NAME)

    val liveFeed = stream.map(_._2)
//    liveFeed.print()
    implicit lazy val formats = org.json4s.DefaultFormats

    val eventHistoryMap = liveFeed
      . map(x =>
                try {
                  parse(x)
                }  catch {
                  case e : Exception =>  logError("[EventStream exception]: " + e.getMessage);  null;
                })
      .filter(x => x != null)
      . map(y => try {
        (
          (y \\ "device_id").extract[String],
          (y \\ "date").extract[String],
          (y \\ "time").extract[String],
          compact(y \\ "latitude"),
          compact(y \\ "longitude"),
          compact(y \\ "altitude"),
          (y \\ "source").extract[String],
          (y \\ "type").extract[String],
          convertAttributes((y \\ "attributes").extract[Map[String, String]]))
        } catch {
            case e: Exception => logError("[EventStream exception]: "+ e.getMessage);  null;
        })
      .filter(x => x != null)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssz")
    //date is not required in event table
    val eventMap = eventHistoryMap.map(x => (x._1, x._3, x._4, x._5, x._6, x._7, x._8, x._9))
    //group by primary key (device_id,source,type)
    val filteredEventMap = eventMap.map(x => ((x._1, x._6, x._7), x))
      .reduceByKey((x, y) => (
          if (dateFormat.parse(x._2).compareTo(dateFormat.parse(y._2)) > 0) x else y)
      ).map(x => x._2)

//    eventHistoryMap.print()

    eventHistoryMap.saveToCassandra(CASSANDRA_KEY_SPACE, EVENT_HISTORY_TABLE,
      SomeColumns("device_id", "date", "time", "lat", "lng", "alt",
        "src", "type", "attributes"))

    filteredEventMap.saveToCassandra(CASSANDRA_KEY_SPACE, EVENT_TABLE,
      SomeColumns("device_id", "time", "lat", "lng", "alt",
        "src", "type", "attributes"))
  }
}
