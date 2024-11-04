package com.flipkart.streaming

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
object GeoTagStream extends Logging {

  var TOPIC_NAME = "geotag_feed"
  var CASSANDRA_KEY_SPACE = "compass"
  var GEOTAG_TABLE = "geotag"

  def convertAttributes(attributes:Map[String,String]):
  List[UDTValue] ={
    var list = new ListBuffer[UDTValue]
    for ((key, value) <- attributes) {
      var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
      list += udtValue
    }
    return list.toList
  }
  def convertAddress(address:List[Map[String,String]]):
  List[UDTValue] ={
    var listBuffer = new ListBuffer[UDTValue]
    for (addr <- address) {
      for ((key, value) <- addr) {
        var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
        listBuffer += udtValue
      }
    }
    return listBuffer.toList
  }


  def run(ssc: StreamingContext, zookeeperQuorum: String) {

    val stream = KafkaUtils.createStream(ssc, zookeeperQuorum, TOPIC_NAME, Map(TOPIC_NAME -> 3))
    logInfo("Reading stream from topic: " + TOPIC_NAME)

    val liveFeed = stream.map(_._2)
//    liveFeed.print()
    implicit lazy val formats = org.json4s.DefaultFormats

    val geoTagStreamMap = liveFeed.map( x =>
      try{ parse(x)} catch { case e:Exception => logError("[GeoTagStream exception]: " + e.getMessage);  null;})
      .filter(x => x!=null)
      .map(y => try{ (
      (y \\ "device_id").extract[String],
      (y \\ "date").extract[String],
      (y \\ "tag_id").extract[String],
      (y \\ "time").extract[String],
      compact(y \\ "latitude"),
      compact(y \\ "longitude"),
      compact(y \\ "altitude"),
      (y \\ "source").extract[String],
      (y \\ "type").extract[String],
      (y \\ "address_hash").extract[String],
      (y \\ "address_full").extract[String],
      convertAddress((y \\ "address").extract[List[Map[String, String]]]),
      convertAttributes((y \\ "attributes").extract[Map[String, String]])
      )}catch {case e:Exception => logError("[GeoTagStream exception]: " + e.getMessage);  null;})
      .filter(x => x!=null)

    geoTagStreamMap.saveToCassandra(CASSANDRA_KEY_SPACE, GEOTAG_TABLE,
      SomeColumns("device_id", "date", "tag_id", "time", "lat", "lng", "alt",
        "src", "type", "addr_hash", "addr_full", "addr", "attributes"))

  }
}
