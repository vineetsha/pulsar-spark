package com.flipkart.streaming

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.jackson.JsonMethods._

/**
 * Created by sharma.varun on 11/12/14.
 */
object SubareaStream extends Logging{
  
  var TOPIC_NAME = "subarea_feed"

  var CASSANDRA_KEY_SPACE = "compass"

  var SUBAREA_TABLE = "subarea"

  def run(ssc:StreamingContext, zookeeperQuorum:String) {

    val stream = KafkaUtils.createStream(ssc, zookeeperQuorum, TOPIC_NAME, Map(TOPIC_NAME -> 3))
    logInfo("Reading stream from topic: " + TOPIC_NAME )
    
    val live_feed = stream.map(_._2)
    live_feed.print()
    implicit lazy val formats = org.json4s.DefaultFormats

    val subarea_stream_map = live_feed.map(x => parse(x)).map(y => (
      compact(y \\ "id").toLong,
      (y \\ "subareaName").extract[String],
      compact(y \\ "hubId").toLong,
      (y \\ "deviceId").extract[String],
      (y \\ "agentId").extract[String],
      compact(y \\ "latitude"),
      compact(y \\ "longitude"),
      compact(y \\ "altitude"),
      (y \\ "time").extract[String]
      ))

    subarea_stream_map.saveToCassandra(CASSANDRA_KEY_SPACE, SUBAREA_TABLE,
      SomeColumns("id", "sa_name", "hub_id", "device_id", "agent_id", "lat", "lng", "alt", "time"))
  }
}
