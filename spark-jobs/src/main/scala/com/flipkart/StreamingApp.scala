package com.flipkart

import com.flipkart.streaming.{EventStream, GeoTagStream, SignatureStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

object StreamingApp extends Logging{
  def main(args: Array[String]){
    if (args.length < 2) {
      System.err.println("Usage: StreamingApp <cassandraHost> <zookeeperQuorum>")
      System.exit(1)
    }
    val appName = "StreamingApp"
    val cassandraHost = args(0)
    val zookeeperQuorum = args(1)
    val sc = new SparkConf().setAppName(appName)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.streaming.unpersist", "true")
      .set("spark.cassandra.input.split.size_in_mb", "67108864")

    setStreamingLogLevels()
    logInfo("Connecting " + appName + " to cassandra : " + cassandraHost)
    logError("testing sentry")
    val ssc = new StreamingContext(sc, Seconds(10))
    
    try {
//      LocationUpdateStream.run(ssc, zookeeperQuorum)
//      DeliveryStream.run(ssc, zookeeperQuorum)
//      SubareaStream.run(ssc, zookeeperQuorum)
//      LandmarkStream.run(ssc, zookeeperQuorum)
      GeoTagStream.run(ssc, zookeeperQuorum)
      EventStream.run(ssc, zookeeperQuorum)
      SignatureStream.run(ssc, zookeeperQuorum)

      ssc.start
      ssc.awaitTermination
    }
    catch{
      case e: Exception =>
        e.printStackTrace()
        logError("Exceptions in executing StreamingApp: " + e.getMessage + e)
    }
    finally {
      sys.ShutdownHookThread {
        logInfo("Gracefully stopping StreamingApp")
        ssc.stop(true, true)
        logInfo("StreamingApp stopped")
      }
    }
  }
  def setStreamingLogLevels() {
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN);
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.WARN);
    val log4jInitialized = org.apache.log4j.Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.INFO)
    }
  }
}
