package com.flipkart.streaming

import java.nio.ByteBuffer

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.jackson.JsonMethods._
import sun.misc.BASE64Decoder

/**
 * Created by sourav.r on 21/04/15.
 */
object SignatureStream extends Logging {

  var TOPIC_NAME = "signature_feed"
  var CASSANDRA_KEY_SPACE = "compass"
  var SIGNATURE_TABLE = "signature"


  def convertToBlob(stream: String): ByteBuffer = {
    return ByteBuffer.wrap(new BASE64Decoder().decodeBuffer(stream))
  }

  def run(ssc: StreamingContext, zookeeperQuorum: String) {

    val stream = KafkaUtils.createStream(ssc, zookeeperQuorum, TOPIC_NAME, Map(TOPIC_NAME -> 3))
    logInfo("Reading stream from topic: " + TOPIC_NAME)

    val liveFeed = stream.map(_._2)
//    liveFeed.print()
    implicit lazy val formats = org.json4s.DefaultFormats

    val signatureMap = liveFeed.map(x =>
      try { parse(x) } catch {case e : Exception =>  logError("[SignatureStream exception]: " + e.getMessage);  null;})
      .filter(x => x!= null)
      .map(y => try { (
      (y \\ "tag_id").extract[String],
      (y \\ "source").extract[String],
      (y \\ "type").extract[String],
      (y \\ "time").extract[String],
      convertToBlob((y \\ "signature").extract[String])
      )} catch {case e: Exception => logError("[SignatureStream exception]: " + e.getMessage);  null;})
      .filter(x => x!= null)

    signatureMap.saveToCassandra(CASSANDRA_KEY_SPACE, SIGNATURE_TABLE,
      SomeColumns("tag_id", "src", "type", "time", "signature")
    )
  }

}
