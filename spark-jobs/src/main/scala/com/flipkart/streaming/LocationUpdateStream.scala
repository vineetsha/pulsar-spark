package com.flipkart.streaming

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.jackson.JsonMethods._
/**
 * Created by sourav.r on 03/12/14.
 */
object LocationUpdateStream extends Logging{

  var TOPIC_NAME = "location_feed"

  var CASSANDRA_KEY_SPACE = "compass"

  var LOCATION_TABLE = "location"

  var LOCATION_HISTORY_TABLE = "location_history"

  def run(streamingContext: StreamingContext, zookeeperQuorum: String) {

    val stream = KafkaUtils.createStream(streamingContext, zookeeperQuorum, TOPIC_NAME, Map(TOPIC_NAME -> 3))
    logInfo("Reading stream from topic: " + TOPIC_NAME )
    
    val live_feed = stream.map(_._2)
    live_feed.print()
    implicit lazy val formats = org.json4s.DefaultFormats

    val location_history_map = live_feed.
      map( try{x => parse(x)} catch {case e : Exception => logError("[LocationUpdateStream exception]: "+ e.getMessage); null;})
      .filter( x => x!=null)
      .map(y =>
        try{
        (
          compact(y \\ "altitude"),
          (y \\ "device_id").extract[String],
          (y \\ "date").extract[String],
          compact(y \\ "network_signal_strength").toInt,
          compact(y \\ "latitude"),
          compact(y \\ "longitude"),
          compact(y \\ "charging_status"),
          compact(y \\ "accuracy_level"),
          (y \\ "time").extract[String],
          (y \\ "agent_id").extract[String],
          compact(y \\ "battery_level"))
      }catch {case e : Exception => logError("[LocationUpdateStream exception]: "+ e.getMessage); null;}
      )
      .filter( x => x!=null)

    val location_map = location_history_map.map(x => (x._1, x._2, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11))

    location_map.saveToCassandra(CASSANDRA_KEY_SPACE, LOCATION_TABLE,
      SomeColumns("alt", "device_id", "signal", "lat", "lng", "charging", "acc", "time", "agent_id", "battery"))

    location_history_map.saveToCassandra(CASSANDRA_KEY_SPACE, LOCATION_HISTORY_TABLE,
      SomeColumns("alt", "device_id", "date", "signal", "lat", "lng", "charging", "acc", "time", "agent_id", "battery"))
  }
}