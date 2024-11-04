package com.flipkart.model

import com.datastax.spark.connector.{ColumnName, SomeColumns}
import com.fasterxml.jackson.annotation.{JsonProperty, JsonIgnoreProperties}
import com.flipkart.utils.JsonUtility

import scala.collection.mutable.ListBuffer

/**
 * Created by sourav.r on 06/04/16.
 */

class TripTrackerModel(var id:String,
                                var active:Boolean,
                                var dest_geofence:String,
                                var last_geofence_id:String,
                                var notify_topic:String,
                                var src_geofence:String,
                                var src:String,
                                var tracking_id:String,
                                var waypoint_geofences:java.util.List[String]
                             ){

  def getWaypoint_geofences():List[Geofence]={
    var geoFences = new ListBuffer[Geofence]()
    for(i <- 0 to waypoint_geofences.size-1){
      geoFences+=(JsonUtility.deserialize[Geofence](this.waypoint_geofences.get(i)))
    }
    geoFences.toList
  }

  def getSrcGeofence():Geofence={
    JsonUtility.deserialize[Geofence](src_geofence)
  }

  def getDestGeofence():Geofence={
    JsonUtility.deserialize[Geofence](dest_geofence)
  }

  def getAllGeofences():List[Geofence]={
    List(getSrcGeofence).++(getWaypoint_geofences).++(List(getDestGeofence))//.++(List(new Geofence(Geofence.outer_geofence.toString,0.0,0.0,0)))
  }
}

object TripTrackerModel{
  def getColumns: SomeColumns ={
    SomeColumns(ColumnName("id"),
      ColumnName("active"),
      ColumnName("dest_geofence"),
      ColumnName("last_geofence_id"),
      ColumnName("notify_topic"),
      ColumnName("src_geofence"),
      ColumnName("src"),
      ColumnName("tracking_id"),
      ColumnName("waypoint_geofences"))
  }

  override def toString = s"TripTrackerModel($getColumns)"
}
/*{
         "geofence_id":"goa_hub",
         "latitude":12.9332084,
         "longitude":77.6836479,
         "radius":1000
      }
 */

@JsonIgnoreProperties(ignoreUnknown = true)
class Geofence(
                var geofence_id:String,
                var latitude:Double,
                var longitude:Double,
                var radius:Double
)

object Geofence extends Enumeration {
  type Geofence = Value
  val  outer_geofence = Value
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Location(
                var latitude:Double,
                var longitude:Double
                )

object TripTrackerAlert extends Enumeration {
  type TripTrackerAlert = Value
  val trip_start, trip_end,geofence_entry,geofence_exit = Value
}
/*
{
   "trip_tracker_id":"triptracker_1",
   "time":"2016-03-09 17:30:25+0530"
   "location":{
      "latitude":12.9332084,
      "longitude":77.6836479
   },
   "geofence":{
      "geofence_id":"MotherHub_BLR",
      "latitude":12.9332084,
      "longitude":77.6836479,
      "radius":500
   },
   "type":"trip_start/trip_end/geofence_entry/geofence_exit"
}
*/
class TripTrackerAlert(var trip_tracker_id: String,
                       var time: String ,
                       var tracking_id: String,
                       var location: Location,
                       var geofence: Geofence,
                       @JsonProperty("type")
                       var _type: String
                        )
