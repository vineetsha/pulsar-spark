package com.flipkart.service

import java.util
import java.util.UUID

import com.datastax.driver.core.{Row, ResultSet, SimpleStatement}
import com.datastax.spark.connector.cql.CassandraConnector
import com.flipkart.core.{GeoTagPayload, EventHistoryPayload}
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.{TripTrackerAlert, Location, Geofence, TripTrackerModel}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.{HttpClient, JsonUtility}
import org.apache.logging.log4j.Level._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
 * Created by sourav.r on 06/04/16.
 */
object TripTrackingService {

  def postToTopic(payload:String, msgQUrl:String) : Unit = {
    Logger.log(this.getClass, INFO, BaseSLog("[TripTrackingService-postToQueue] " +" msgQUrl:" + msgQUrl + " payload: " + payload))
    val msg_id = UUID.randomUUID().toString
    val group_id = UUID.randomUUID().toString
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    headers.put("X_RESTBUS_MESSAGE_ID",msg_id)
    headers.put("X_RESTBUS_GROUP_ID", group_id)
    headers.put("X_RESTBUS_HTTP_METHOD", "POST")
    val response = HttpClient.INSTANCE.executePost(msgQUrl, payload, headers)
    Logger.log(this.getClass, INFO, BaseSLog("post response headers: " + response.getStatusCode + " " + response.getHeaders))
    Logger.log(this.getClass, INFO, BaseSLog("post response body: " + response.getResponseBody))
    if (response.getStatusCode != 200) {
      Logger.log(this.getClass, ERROR, BaseSLog("[TripTrackingService-postToQueue] Error : Returned status code " + response.getStatusCode))
      throw new RuntimeException
    }
  }

  def queryCassandra( cassandraConnector: CassandraConnector, queryStatement:SimpleStatement ): ResultSet = {
    Logger.log(this.getClass, INFO, BaseSLog("[TripTrackingService-queryCassandra] queryStatement" + queryStatement.getQueryString()))
    cassandraConnector.withSessionDo(session => {
      return session.execute(queryStatement)
    })
  }

  def repartitionStream(data_rdd: DStream[(String,String)], repartition: Boolean, numPartitions: Int) = {
    if (repartition) data_rdd.repartition(numPartitions) else data_rdd
  }

  def applyFunctionForEachRDD(repartition:Boolean, numPartitions:Int, data_rdd: DStream[(String,String)],
                              zkProperties:Map[String, String], configMap:Map[String, String],
                              f:(RDD[(String,String)], Map[String, String], Map[String, String])=> Unit):Unit ={
    data_rdd.foreachRDD(rdd => {
      if(repartition) {
        f(rdd.partitionBy(new HashPartitioner(numPartitions)).map(x=>x), zkProperties, configMap)
      }else{
        f(rdd, zkProperties, configMap)
      }
    })
  }

  def haversineDistanceInKM(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLong = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    6371 * greatCircleDistance * 1000
  }

  def getTripTrackerId(cassandraConnector: CassandraConnector, tracking_id: String):String = {
    val resultSetRow = queryCassandra(cassandraConnector,
      new SimpleStatement("SELECT * FROM compass.trip_tracker_mapping WHERE tracking_id = ?", tracking_id)).all()
    for( i <- 0 to resultSetRow.size-1){
      if(resultSetRow.get(i).getBool("active") == true){
        return resultSetRow.get(i).getString("trip_tracker_id")
      }
    }
    return null
  }

  def getTripTrackerIds(cassandraConnector: CassandraConnector, tracking_id: String): List[String] = {
    var tripTrackerIds = new ListBuffer[String]()
    val resultSetRow = queryCassandra(cassandraConnector,
      new SimpleStatement("SELECT * FROM compass.trip_tracker_mapping WHERE tracking_id = ?", tracking_id)).all()
    for( i <- 0 to resultSetRow.size-1){
      if(resultSetRow.get(i).getBool("active") == true){
        tripTrackerIds+=resultSetRow.get(i).getString("trip_tracker_id")
      }
    }
    return tripTrackerIds.toList
  }

  def getTripTracker(cassandraConnector: CassandraConnector, id: String):TripTrackerModel={
    val resultSetRow = queryCassandra(cassandraConnector,
      new SimpleStatement("SELECT * FROM compass.trip_tracker WHERE id= ?", id)).one()
    if(resultSetRow != null) {
      new TripTrackerModel(resultSetRow.getString("id"),
        resultSetRow.getBool("active"),
        resultSetRow.getString("dest_geofence"),
        resultSetRow.getString("last_geofence_id"),
        resultSetRow.getString("notify_topic"),
        resultSetRow.getString("src_geofence"),
        resultSetRow.getString("src"),
        resultSetRow.getString("tracking_id"),
        resultSetRow.getObject("waypoint_geofences").asInstanceOf[java.util.List[String]]
      )
    }else{
      return null
    }
  }

  def getGeofences(tripTracker: TripTrackerModel):List[Geofence]={
    var geoFences = new ListBuffer[Geofence]()
    for(i <- 0 to tripTracker.waypoint_geofences.size-1){
      geoFences+=(JsonUtility.deserialize[Geofence](tripTracker.waypoint_geofences.apply(i)))
    }
    geoFences.toList
  }


  def isInsideGeofences(geofence: Geofence, location: Location): Boolean = {
    if (haversineDistanceInKM((geofence.latitude, geofence.longitude), (location.latitude, location.longitude)) < geofence.radius) {
      return true
    } else {
      return false
    }
  }

  def publishTripAlertToTopic(tripTracker: TripTrackerModel, location: Location,
                              breachedGeofence: Geofence, alertType: TripTrackerAlert.Value):Unit ={
    postToTopic(
      JsonUtility.serialize(
        new TripTrackerAlert(tripTracker.id,DateTime.now().toDateTimeISO.toString(),tripTracker.tracking_id,location,breachedGeofence,alertType.toString)),
      tripTracker.notify_topic)
  }

  def updateTripTracker(cassandraConnector: CassandraConnector, tripTracker: TripTrackerModel, lastGeofenceId: String): Unit ={
    queryCassandra(cassandraConnector,
      new SimpleStatement("UPDATE compass.trip_tracker SET last_geofence_id = ? WHERE id = ?",lastGeofenceId, tripTracker.id))
  }

  def publishTripAlert(tripTracker: TripTrackerModel, current_loc:Location, cassandraConnector: CassandraConnector): Unit = {
    var currentGeofenceId:String = Geofence.outer_geofence.toString
    var geofences:List[Geofence] = tripTracker.getAllGeofences
    var geofenceMap = HashMap[String,Geofence]()
    for(i <- 0 to geofences.size-1){
      var tmpGeofence = geofences.apply(i)
      geofenceMap+=(tmpGeofence.geofence_id -> tmpGeofence)
      if(isInsideGeofences(geofences.apply(i), current_loc)) {
        currentGeofenceId = geofences.apply(i).geofence_id
      }
    }

    if(currentGeofenceId != tripTracker.last_geofence_id) {
      if (currentGeofenceId != Geofence.outer_geofence.toString) {
        if (currentGeofenceId != tripTracker.last_geofence_id) {
          if(tripTracker.last_geofence_id == tripTracker.getSrcGeofence.geofence_id){
            publishTripAlertToTopic(tripTracker, current_loc,geofenceMap.apply(tripTracker.last_geofence_id), TripTrackerAlert.trip_start)
          }else if(tripTracker.last_geofence_id != Geofence.outer_geofence.toString){
            publishTripAlertToTopic(tripTracker, current_loc,geofenceMap.apply(tripTracker.last_geofence_id), TripTrackerAlert.geofence_exit)
          }

          if (currentGeofenceId == tripTracker.getDestGeofence.geofence_id) {
            //publish trip_end for geofenceId
            publishTripAlertToTopic(tripTracker, current_loc,geofenceMap.apply(currentGeofenceId), TripTrackerAlert.trip_end)
          } else {
            //publish geofence_entry for geofenceId
            publishTripAlertToTopic(tripTracker, current_loc,geofenceMap.apply(currentGeofenceId), TripTrackerAlert.geofence_entry)
          }
        }
      } else {
        if (tripTracker.last_geofence_id == tripTracker.getSrcGeofence.geofence_id) {
          //publish trip_start tripTracker.last_geofence_id
          publishTripAlertToTopic(tripTracker, current_loc,geofenceMap.apply(tripTracker.last_geofence_id), TripTrackerAlert.trip_start)
        } else if (tripTracker.last_geofence_id != Geofence.outer_geofence.toString) {
          //publish geofence_exit for tripTracker.last_geofence_id
          publishTripAlertToTopic(tripTracker, current_loc,geofenceMap.apply(tripTracker.last_geofence_id), TripTrackerAlert.geofence_exit)
        }
      }
      //tripTracker.last_geofence_id = currentGeofenceId
      updateTripTracker(cassandraConnector,tripTracker, currentGeofenceId)
    }
  }

  def processEventForTripAlert(data_rdd: RDD[(String,String)], zkProperties:Map[String, String],
                                    configMap:Map[String, String]): Unit = {
    try {
      val conf = data_rdd.sparkContext.getConf
      val cassandraConnector = CassandraConnector(conf)
      var payloads = JsonUtility.deserialize[EventHistoryPayload](data_rdd.map(_._2))
      payloads = payloads.keyBy(x => x.device_id).partitionBy(new HashPartitioner(60)).map(x=>x._2)
      val streamMap = payloads.map(payload=>{
         var tripTrackerIds = getTripTrackerIds(cassandraConnector,payload.device_id)
         for (tripTrackerId <- tripTrackerIds) {
           if (tripTrackerId != null) {
             var tripTracker = getTripTracker(cassandraConnector, tripTrackerId)
             publishTripAlert(tripTracker, new Location(payload.latitude, payload.longitude), cassandraConnector)
           }
         }
      })
      streamMap.count()
    }catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while processing RDD : " +  e.getMessage, e))
    }finally {
      if(zkProperties!= null) {
        ZookeeperManager.updateOffsetsinZk(zkProperties, data_rdd)
      }
    }
  }
}
