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
object DeliveryStream extends Logging{
  
  var TOPIC_NAME = "shipment_feed"

  var CASSANDRA_KEY_SPACE = "compass"

  var DELIVERY_TABLE = "delivery"

  def run(ssc:StreamingContext, zookeeperQuorum:String) {

    val stream = KafkaUtils.createStream(ssc, zookeeperQuorum, TOPIC_NAME, Map(TOPIC_NAME -> 3))
    logInfo("Reading stream from topic: " + TOPIC_NAME )
    
    val live_feed = stream.map(_._2)
    live_feed.print()
    implicit lazy val formats = org.json4s.DefaultFormats

    val delivery_stream_map = live_feed.
      map( try {x => parse(x)} catch {case e : Exception => logError("[DeliveryStream exception]: "+ e.getMessage); null;})
      .filter( x => x!=null)
      .map(y =>
      try {
      (
      (y \\ "device_id").extract[String],
      (y \\ "date").extract[String],
      (y \\ "agent_id").extract[String],
      (y \\ "time").extract[String],
      compact(y \\ "latitude"),
      compact(y \\ "longitude"),
      compact(y \\ "altitude"),
      compact(y \\ "accuracy_level"),
      compact(y \\ "network_signal_strength").toInt,
      (y \\ "event_type").extract[String],
      (y \\ "runsheet_id").extract[String],
      (y \\ "shipment_id").extract[String],
      (y \\ "address_id").extract[String],
      (y \\ "address").extract[String]
      )} catch {case e : Exception => logError("[DeliveryStream exception]: "+ e.getMessage); null;})
      .filter( x => x!=null)

    delivery_stream_map.saveToCassandra(CASSANDRA_KEY_SPACE, DELIVERY_TABLE,
      SomeColumns("device_id", "date", "agent_id", "time", "lat", "lng", "alt", "acc",
        "signal", "event_type", "runsheet_id","shipment_id","addr_id","addr"))
  }
}
