package com.flipkart.service

import java.time.{LocalDateTime, ZoneOffset}
import java.util.Calendar

import com.datastax.spark.connector.rdd.{CassandraLeftJoinRDD, CassandraTableScanRDD}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.{SomeColumns, _}
import com.flipkart.model.RefinedDeliveryLocationResult
import com.flipkart.service.GeoTagOptimizedService.{validate_attributes}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.flipkart.utils.{BestLatLngCalculator, DateUtils}
import org.apache.logging.log4j.Level.{ERROR, INFO}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ConstantInputDStream

import scala.collection.mutable


/**
  * Created by raunaq.singh on 05/08/20.
  **/


object DeliveryLocationRefinementService {

  def streamDeliveryPings(ssc: StreamingContext, refinedDeliveryDataDStream: ConstantInputDStream[CassandraRow],
                          pingsTriangulationConfig: Map[String, String], clusterScoreConfig: Map[String, String]){
    try {
      refinedDeliveryDataDStream.foreachRDD(rdd =>{
        val now = LocalDateTime.now()
        val currMillis = now.toInstant(ZoneOffset.ofHoursMinutes(5,30)).toEpochMilli
        val currMinusOneHourMillis: Long = now.minusHours(1).toInstant(ZoneOffset.ofHoursMinutes(5,30)).toEpochMilli
        val dayStartMillis: Long = now.toLocalDate.atStartOfDay().toInstant(ZoneOffset.ofHoursMinutes(5,30)).toEpochMilli
        val currDate = DateUtils.dateFormatter.format(now)
        val refinedDataRDD = ssc.cassandraTable("compass", "refined_delivered_data").select("date", "timestamp", "device_id", "addr_hash", "accuracy", "agent_id", "delivered_lat", "delivered_lng", "is_refined")
          .where("date = ? and timestamp >= ? and timestamp <= ?", currDate, dayStartMillis, currMinusOneHourMillis)
        val deliveryDataRDD = refinedDataRDD.map(x => (x.getString("date"), x.getLong("timestamp"), x.getString("device_id"),
          x.getString("addr_hash"), x.getDouble("accuracy"), x.getString("agent_id"), x.getDouble("delivered_lat"),
          x.getDouble("delivered_lng"), x.getBoolean("is_refined"))).filter(x => !x._9)
        val delivery_agent_data = deliveryDataRDD.map(x => x._6).distinct().collect().toList
        val pingsDataRDD = get_pings_data(ssc, delivery_agent_data, dayStartMillis, currMillis, pingsTriangulationConfig)
        val agentWiseDeliveryPingsDataRDD = deliveryDataRDD.keyBy(_._6).join(pingsDataRDD)
        val deliveryPingsDataRDD = agentWiseDeliveryPingsDataRDD.map(x => x._2)
        refine_delivered_location(ssc, deliveryPingsDataRDD, pingsTriangulationConfig, clusterScoreConfig)
      })
    } catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in executing Delivery Location Refinement App: " + e.getMessage, e))
        throw e
    }
  }

  def get_pings_data(ssc: StreamingContext, agent_ids: List[String], fromMillis: Long, toMillis: Long, pingsTriangulationConfig: Map[String, String]): RDD[(String, List[(Long, Double, Double, Double, Double, Double)])] = {
    Logger.log(this.getClass, INFO, BaseSLog(s"Getting pings data for agents from $fromMillis to $toMillis"))
    val pingsDataRDD: CassandraTableScanRDD[CassandraRow] = ssc.cassandraTable("gps", "pings")
      .select("entity_id", "latitude", "longitude", "accuracy", "speed", "battery", "client_timestamp")
      .where("entity_id IN ? and entity_type = 'AGENT' and tenant = 'CL' and client_timestamp >= ? and client_timestamp <= ?", agent_ids,  fromMillis, toMillis)
    val deliveryPingsRDD: RDD[(String, (Long, Double, Double, Double, Double, Double))] = pingsDataRDD.map(x => (x.getString("entity_id"), (x.getLong("client_timestamp"), x.getDouble("latitude"), x.getDouble("longitude"), x.getDouble("accuracy"), x.getDouble("speed"), x.getDouble("battery"))))
    val agentGroupedPingsRDD = get_combined_rdd_for_agent_id(deliveryPingsRDD)
    deliveryPingsRDD.unpersist()
    agentGroupedPingsRDD
  }

  def get_combined_rdd_for_agent_id(deliveryPingsRDD: RDD[(String, (Long, Double, Double, Double, Double, Double))]):
  RDD[(String, List[(Long, Double, Double, Double, Double, Double)])] = {
    Logger.log(this.getClass, INFO, BaseSLog(s"Combining delivered pings RDD on Agent ID"))
    val deliveryPingsCombinedRDD = deliveryPingsRDD.combineByKey(
      // createCombiner: add first value to a list
      (x: (Long, Double, Double, Double, Double, Double)) => List(x),
      // mergeValue: add new value to existing .l list
      (acc: List[(Long, Double, Double, Double, Double, Double)], x) => x :: acc,
      // mergeCombiner: combine the 2 lists
      (acc1: List[(Long, Double, Double, Double, Double, Double)],
       acc2: List[(Long, Double, Double, Double, Double, Double)]) => acc1 ::: acc2
    )
    deliveryPingsCombinedRDD
  }

  def refine_delivered_location(ssc: StreamingContext, pingsDataRDD: RDD[((String, Long, String, String, Double, String, Double, Double, Boolean),
    List[(Long, Double, Double, Double, Double, Double)])], pingsTriangulationConfig: Map[String, String], clusterScoreConfig: Map[String, String]): Unit = {

    val refinedDeliveredDataRDD = pingsDataRDD.map(x => (x._1._1, x._1._2, x._1._3,
      DeliveryLocationRefinementService.get_refined_lat_lng(x._1._7, x._1._8, x._1._2, x._1._5, x._2, pingsTriangulationConfig), x._1._4, x._1._7, x._1._8))

    val filteredRefinedDataRDD = refinedDeliveredDataRDD.filter(x=> x._4._1).map(x=> (x._5, (x._6, x._7, x._4._2)))

    save_refined_location_to_optimized_table(ssc, filteredRefinedDataRDD , clusterScoreConfig)

    val refinedDeliveredDataCassandraRDD = refinedDeliveredDataRDD.map(x => (x._1, x._2, x._3, x._4._1, x._4._2)).filter(x => x._1 != null && x._3 != null)
    Logger.log(this.getClass, INFO, BaseSLog(s"Saving $refinedDeliveredDataCassandraRDD to refined_delivered_data in cassandra"))
    refinedDeliveredDataCassandraRDD.saveToCassandra("compass", "refined_delivered_data", SomeColumns("date", "timestamp", "device_id", "is_refined", "algo_wise_refined_data"))
    Logger.log(this.getClass, INFO, BaseSLog(s"Saved $refinedDeliveredDataCassandraRDD RDD to refined_delivered_data in cassandra"))
  }

  def get_refined_lat_lng(del_lat: Double, del_lng: Double, del_time: Long, del_accuracy: Double,
                          pings: List[(Long, Double, Double, Double, Double, Double)], pingsTriangulationConfig: Map[String, String]): (Boolean, List[RefinedDeliveryLocationResult]) = {
    val rangeStart: Long = pingsTriangulationConfig.getOrElse("pingsWindowStartInMillis", "-120000").toLong
    val rangeEnd: Long = pingsTriangulationConfig.getOrElse("pingsWindowEndInMillis", "300000").toLong
    val dbscan_eps = pingsTriangulationConfig.getOrElse("dbScanEps", "50").toInt
    val dbscan_minCount = pingsTriangulationConfig.getOrElse("dbScanMinCount", "3").toInt
    val pings_accuracy_threshold = pingsTriangulationConfig.getOrElse("pingsAccuracyThreshold", "100").toInt
    val timestamp = DateUtils.dateTimeFormatter.format(LocalDateTime.now)

    val filtered_pings = pings.filter(x => x._4 <= pings_accuracy_threshold && x._1 >= del_time + rangeStart && x._1 <= del_time + rangeEnd)
    val dbscan_clusters = get_dbscan_clusters(filtered_pings, dbscan_eps, dbscan_minCount)
    if(dbscan_clusters.isEmpty){
      Logger.log(this.getClass, INFO, BaseSLog(s"Unable to determine a proper cluster from the pings"))
      (false, List(RefinedDeliveryLocationResult.apply(del_lat, del_lng, 0, timestamp, "ORIGINAL_DELIVERED_LOCATION")))
    }
    else {
      val best_cluster = get_best_cluster(del_lat, del_lng, del_time, del_accuracy, dbscan_clusters, pingsTriangulationConfig)
      if(best_cluster.isEmpty) {
        Logger.log(this.getClass, INFO, BaseSLog(s"Unable to locate the best cluster from the set of clusters"))
        (false, List(RefinedDeliveryLocationResult.apply(del_lat, del_lng, 0, timestamp, "ORIGINAL_DELIVERED_LOCATION")))
      }
      else {
        val refined_lat_lng = get_best_lat_lng(best_cluster, pingsTriangulationConfig)
        if(refined_lat_lng.isEmpty) {
          Logger.log(this.getClass, INFO, BaseSLog(s"Unable to locate the best lat-lng from the chosen cluster"))
          (false, List(RefinedDeliveryLocationResult.apply(del_lat, del_lng, 0, timestamp, "ORIGINAL_DELIVERED_LOCATION")))
        }
        else {
          Logger.log(this.getClass, INFO, BaseSLog(s"Refined delivery location result: $refined_lat_lng"))
          (true, refined_lat_lng)
        }
      }
    }
  }

  //Runs the DBSCAN algorithm to cluster the pings based on proximity and time range
  def get_dbscan_clusters(pings_data: List[(Long, Double, Double, Double, Double, Double)], eps: Int, minCount: Int)
  : List[(List[(Double, Double, Double, Long)], Double, Double, Long)] = {
    val clusters = new mutable.ListBuffer[(List[(Double, Double, Double, Long)], Double, Double, Long)]()
    val current_cluster = new mutable.ListBuffer[(Double, Double, Double, Long)]()
    val sorted_pings = pings_data.sortBy(_._1)
    var centroid_lat: Double = 0
    var centroid_lng: Double = 0
    var avg_cluster_timestamp: Long = 0

    if(pings_data.isEmpty) {
      clusters.toList
    }

    Logger.log(this.getClass, INFO, BaseSLog(s"Clustering pings using a variation of DBSCAN to account for time of ping"))
    for (i <- sorted_pings.indices) {
      if (current_cluster.isEmpty ||
        BestLatLngCalculator.vincenty_distance(sorted_pings(i)._2, sorted_pings(i)._3, current_cluster.head._1, current_cluster.head._2) <= eps) {
        current_cluster.+=:(sorted_pings(i)._2, sorted_pings(i)._3, sorted_pings(i)._4, sorted_pings(i)._1)
        centroid_lat += sorted_pings(i)._2
        centroid_lng += sorted_pings(i)._3
        avg_cluster_timestamp += sorted_pings(i)._1
      } else if (BestLatLngCalculator.vincenty_distance(sorted_pings(i)._2, sorted_pings(i)._3, current_cluster.last._1, current_cluster.last._2) > eps) {
        if (current_cluster.size >= minCount) {
          clusters.append((current_cluster.toList, centroid_lat/current_cluster.size, centroid_lng/current_cluster.size, avg_cluster_timestamp/current_cluster.size))
        }
        centroid_lat = 0
        centroid_lng = 0
        avg_cluster_timestamp = 0
        current_cluster.clear()
        current_cluster.+=:(sorted_pings(i)._2, sorted_pings(i)._3, sorted_pings(i)._4, sorted_pings(i)._1)
        centroid_lat += sorted_pings(i)._2
        centroid_lng += sorted_pings(i)._3
        avg_cluster_timestamp += sorted_pings(i)._1
      }
    }

    if (current_cluster.size >= minCount) {
      clusters.append((current_cluster.toList, centroid_lat/current_cluster.size, centroid_lng/current_cluster.size, avg_cluster_timestamp/current_cluster.size))
    }

    clusters.toList
  }

  //Selects the best cluster from the set of clusters based on distance from delivered location and time proximity using avg. cluster timestamp.
  def get_best_cluster(del_lat: Double, del_lng: Double, del_time: Long, del_accuracy: Double,
                       clusters: List[(List[(Double, Double, Double, Long)], Double, Double, Long)], pingsTriangulationConfig: Map[String, String])
  : List[(Double, Double, Double, Long)] = {
    var min_distance = Double.MaxValue
    var min_time_diff = Long.MaxValue
    var best_cluster: List[(Double, Double, Double, Long)] = List()
    val del_accuracy_threshold = pingsTriangulationConfig.getOrElse("deliveredAccuracyThreshold", "100").toInt
    val cluster_del_distance_threshold = pingsTriangulationConfig.getOrElse("clusterToDelLocationDistanceThreshold", "100").toInt

    Logger.log(this.getClass, INFO, BaseSLog(s"Determining best cluster based on cluster centroid distance from " +
      s"delivered location and average cluster time difference from delivery time"))
    for (i <- clusters.indices) {
      if(del_accuracy <= del_accuracy_threshold){
        val centroid_distance = BestLatLngCalculator.vincenty_distance(clusters(i)._2, clusters(i)._3, del_lat, del_lng)
        if(centroid_distance <= min_distance && centroid_distance <= cluster_del_distance_threshold){
          min_distance = centroid_distance
          best_cluster = clusters(i)._1
        }
      }else{
        if(math.abs(clusters(i)._4 - del_time) <= min_time_diff){
          min_time_diff = math.abs(clusters(i)._4 - del_time)
          best_cluster = clusters(i)._1
        }
      }
    }

    best_cluster
  }

  //Determines the best lat-lng from the cluster, using 2 methods, Geocode Score logic and centroid logic
  def get_best_lat_lng(cluster: List[(Double, Double, Double, Long)], pingsTriangulationConfig: Map[String, String]): List[RefinedDeliveryLocationResult] ={
    Logger.log(this.getClass, INFO, BaseSLog(s"Determining best lat-lng from chosen cluster"))
    val refined_results = new mutable.ListBuffer[RefinedDeliveryLocationResult]()
    val cluster_score_lat_lng = BestLatLngCalculator.get_cluster_best_lat_lng_with_score(cluster, pingsTriangulationConfig)
    val centroid_lat_lng = BestLatLngCalculator.get_cluster_centroid_lat_lng(cluster)
    val timestamp = DateUtils.dateTimeFormatter.format(LocalDateTime.now)
    if(cluster_score_lat_lng != null){
      if(cluster_score_lat_lng._4)
        refined_results.append(RefinedDeliveryLocationResult.apply(cluster_score_lat_lng._1, cluster_score_lat_lng._2, 0, timestamp,"CLUSTER_GEOCODE_SCORE_HIGH"))
      else
        refined_results.append(RefinedDeliveryLocationResult.apply(cluster_score_lat_lng._1, cluster_score_lat_lng._2, 0, timestamp,"CLUSTER_GEOCODE_SCORE_LOW"))
    }
    if(centroid_lat_lng != null){
        refined_results.append(RefinedDeliveryLocationResult.apply(centroid_lat_lng._1, centroid_lat_lng._2, 0, timestamp,"CLUSTER_CENTROID"))
    }

    refined_results.toList
  }

  def save_refined_location_to_optimized_table(ssc: StreamingContext, filteredRefinedDataRDD: RDD[(String, (Double, Double, List[RefinedDeliveryLocationResult]))], clusterScoreConfig: Map[String, String]) {

    val CASSANDRA_KEY_SPACE = "compass"
    val GEOTAG_OPTIMIZED_LOOKUP_TABLE = "geotag_optimized_lookup"

    val cassandraJoinedRDD= filteredRefinedDataRDD.leftJoinWithCassandraTable(CASSANDRA_KEY_SPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE).select("addr_hash", "attributes", "addr").on(SomeColumns("addr_hash"))

    val cassandraExpanded = cassandraJoinedRDD.map(x=> (x._1,
      x._2.map(y=>(y.getString("addr_hash"), validate_attributes(y.getList[UDTValue]("attributes")), y.getList[UDTValue]("addr")))
        .map(z=>z._2.toList.map(k=>
      ( if (k.getStringOption("time").isDefined)
      {k.getString("time")} else
      {new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance.getTime)},
        k.getDouble("lat"), k.getDouble("lng"),
        if (k.getDoubleOption("accuracy_level").isDefined)
        {k.getDouble("accuracy_level")} else {198},
        z._3.toList)))))

    val cassandraJoinedExpanded = cassandraExpanded.map(x=> (x._1._1, (x._1._2, x._2.get)))

    val geotagOptimizedWithRefinedDataRDD = cassandraJoinedExpanded.map(x=> (x._1, removeDelValuesAndAddRefinedLocation(x._2._2, x._2._1._1, x._2._1._2, x._2._1._3)))

    val best_location_with_cluster_score_rdd = geotagOptimizedWithRefinedDataRDD.map(x=> (x._1,  BestLatLngCalculator.get_best_lat_lng_with_cluster_score(x._1, x._2, clusterScoreConfig), x._2))

    val geoTag_optimized_cassandra_model_rdd = best_location_with_cluster_score_rdd.map(x => (x._1, x._3.map(y=>(y._2,y._3,y._4,y._1)), x._2._1, x._2._2, x._2._3))

    geoTag_optimized_cassandra_model_rdd.saveToCassandra(CASSANDRA_KEY_SPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE,
      SomeColumns("addr_hash", "attributes", "lat", "lng", "cluster_score"))

  }

  def removeDelValuesAndAddRefinedLocation(vec: List[(String, Double, Double, Double, List[UDTValue])], del_lat: Double, del_lng: Double, refined_lat_lng: List[RefinedDeliveryLocationResult]): List[(String, Double, Double, Double, List[UDTValue])] ={

    var finalList = new mutable.ListBuffer[(String, Double, Double, Double, List[UDTValue])]()

    Logger.log(this.getClass, INFO, BaseSLog(s"Removing Del Lat Long from list"))

    for (i <- vec.indices){
      if(vec(i)._2!=del_lat && vec(i)._3!=del_lng){
        finalList+=vec(i)
      }
    }

    Logger.log(this.getClass, INFO, BaseSLog(s"Adding Refined Lat Long To list"))
    for (i <- refined_lat_lng.indices){
      var refined = (refined_lat_lng(i).time, refined_lat_lng(i).lat, refined_lat_lng(i).lng, 198.0, vec.head._5)
      finalList+=refined
    }

    val finalLength = finalList.length

    Logger.log(this.getClass, INFO, BaseSLog(s"After Addition of new Refined Location Length : $finalLength"))

    if(finalLength > 100){
      finalList = finalList.takeRight(100)
    }

    finalList.toList
  }
}

