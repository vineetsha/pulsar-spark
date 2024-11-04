package com.flipkart.utils

import com.datastax.spark.connector._
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level.INFO
import org.apache.lucene.util.GeoDistanceUtils
import scala.util.control.Breaks._
import scala.collection.mutable

/**
 * Created by akshay.rm on 08/08/19.
 */

object BestLatLngCalculator {

  def vincenty_distance(lat_1:Double, lng_1:Double, lat_2:Double, lng_2:Double): Double ={

    if(lat_1 == 0 || lat_2 ==0)
      return 10000000
    val dist = GeoDistanceUtils.vincentyDistance(lng_1, lat_1, lng_2, lat_2)
    dist

  }

  def get_is_high_confidence_from_cluster_score(score: Double) : Boolean = {

    if (score == 1.0)
      true
    else
      false
  }

  def get_best_lat_lng(addr_hash: String, vec: List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])]): (Double, Double) = {

      val length = vec.length

      Logger.log(this.getClass, INFO, BaseSLog(s"Calculating Best Lat Long for List of length : $length for addr_hash : $addr_hash"))

      if (length < 4 || length > 500){
        return (vec(length-1)._2, vec(length-1)._3)
      }
      var count_list = new mutable.ListBuffer[Integer]()

      for (i <- vec.indices){
        count_list+=0
      }
      var max_index = 0
      for (i <- vec.indices){
        for (j <- vec.indices) {
          if (vincenty_distance(vec(i)._2, vec(i)._3, vec(j)._2, vec(j)._3) < 300)
            count_list.update(i, count_list(i)+1)
        }
        if (count_list(max_index) < count_list(i))
          max_index = i
      }
      (vec(max_index)._2, vec(max_index)._3)

  }

  def get_best_lat_lng_with_cluster_score_v2(addr_hash: String, vec: List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])], clusterScoreConfig: Map[String, String]): (Double, Double, Double) = {
    val vec2 = vec.map(x=> (x._1, x._2, x._3, x._4, x._5));
    get_best_lat_lng_with_cluster_score(addr_hash, vec2, clusterScoreConfig);
  }

  def get_best_lat_lng_with_cluster_score(addr_hash: String, vec: List[(String, Double, Double, Double, List[UDTValue])], clusterScoreConfig: Map[String, String]): (Double, Double, Double) = {
    val clusterScoreDistanceThreshold = clusterScoreConfig.getOrElse("distanceThreshold", "200").toDouble
    val clusterScoreMinPointsCount = clusterScoreConfig.getOrElse("minPointsCount", "3").toDouble
    val clusterScoreMajorityPercentage = clusterScoreConfig.getOrElse("majorityPercentage", "50").toDouble

    val numPoints = vec.length
    val locationScoreMap = mutable.HashMap[(Double, Double), Double]()
    val locationNeighbourCountMap = mutable.HashMap[(Double, Double), Integer]()

    Logger.log(this.getClass, INFO, BaseSLog(s"Calculating best lat lng with geocode score for list of length = $numPoints for addr_hash : $addr_hash"))

    //if number of points is less than 3 then return last delivered location
    if (numPoints < 3) {
      return (vec.last._2, vec.last._3, 0.0)
    }

    for (i <- vec.indices){
      locationScoreMap.put((vec(i)._2, vec(i)._3), 0.0)
      locationNeighbourCountMap.put((vec(i)._2, vec(i)._3), 0)
    }

    for (i <- vec.indices){
      for (j <- vec.indices) {
        if ((vec(i)._2, vec(i)._3) != (vec(j)._2, vec(j)._3)) {
          val tempDistance: Double = vincenty_distance(vec(i)._2, vec(i)._3, vec(j)._2, vec(j)._3)
          locationScoreMap.put((vec(i)._2, vec(i)._3), 1.0 / (1.0 + tempDistance))
          if (tempDistance <= clusterScoreDistanceThreshold) {
            locationNeighbourCountMap.put((vec(i)._2, vec(i)._3), locationNeighbourCountMap((vec(i)._2, vec(i)._3)) + 1)
          }
        }
      }
    }

    //get locations with max Scores
    val maxScoreLocationsList = locationScoreMap.filter(_._2 == locationScoreMap.values.max).toList
    var maxNeighbors = 0
    var bestLocation = maxScoreLocationsList.head._1
    var isHighConfidence = false

    // checking if highest score locations have at least half of the delivered locations within 200m range or not
    // if there's a tie resolve it by the location having max neighbors within 200m range
    for(i <- maxScoreLocationsList.indices){
      if(locationNeighbourCountMap(maxScoreLocationsList(i)._1) >= math.ceil(numPoints/2).toInt
        && locationNeighbourCountMap(maxScoreLocationsList(i)._1) > maxNeighbors){
          maxNeighbors = locationNeighbourCountMap(maxScoreLocationsList(i)._1)
          bestLocation = maxScoreLocationsList(i)._1
          isHighConfidence = true
      }
    }
    Logger.log(this.getClass, INFO, BaseSLog(s"Best location after geocode score logic = $bestLocation"))

    if (isHighConfidence) {
      (bestLocation._1, bestLocation._2, 1.0)
    } else {
      (bestLocation._1, bestLocation._2, 0.0)
    }
  }

  def get_cluster_best_lat_lng_with_score(cluster: List[(Double, Double, Double, Long)], pingsTriangulationConfig: Map[String, String]): (Double, Double, Double, Boolean) = {
    val clusterScoreDistanceThreshold = pingsTriangulationConfig.getOrElse("clusterScoreDistanceThreshold", "200").toDouble
    val clusterScoreMinPointsCount = pingsTriangulationConfig.getOrElse("clusterScoreMinPointsCount", "3").toDouble
    val clusterScoreMajorityPercentage = pingsTriangulationConfig.getOrElse("clusterScoreMajorityPercentage", "50").toDouble

    val numPoints = cluster.length
    val locationScoreMap = mutable.HashMap[(Double, Double), Double]()
    val locationNeighbourCountMap = mutable.HashMap[(Double, Double), Integer]()

    Logger.log(this.getClass, INFO, BaseSLog(s"Calculating best cluster lat-lng with geocode score for list of length = $numPoints"))

    for (i <- cluster.indices){
      locationScoreMap.put((cluster(i)._1, cluster(i)._2), 0.0)
      locationNeighbourCountMap.put((cluster(i)._1, cluster(i)._2), 0)
    }

    for (i <- cluster.indices){
      for (j <- cluster.indices) {
        if ((cluster(i)._1, cluster(i)._2) != (cluster(j)._1, cluster(j)._2)) {
          val tempDistance: Double = vincenty_distance(cluster(i)._1, cluster(i)._2, cluster(j)._1, cluster(j)._2)
          locationScoreMap.put((cluster(i)._1, cluster(i)._2), 1.0 / (1.0 + tempDistance))
          if (tempDistance <= clusterScoreDistanceThreshold) {
            locationNeighbourCountMap.put((cluster(i)._1, cluster(i)._2), locationNeighbourCountMap((cluster(i)._1, cluster(i)._2)) + 1)
          }
        }
      }
    }

    val bestLocation: (Double, Double) = locationNeighbourCountMap.maxBy(_._2)._1
    Logger.log(this.getClass, INFO, BaseSLog(s"Best location after geocode score logic = $bestLocation"))

    if (numPoints < clusterScoreMinPointsCount) {
      (bestLocation._1, bestLocation._2, 0.0, get_is_high_confidence_from_cluster_score(0.0))
    } else {
      if (locationNeighbourCountMap(bestLocation) >= math.ceil((clusterScoreMajorityPercentage / 100) * numPoints).toInt) {
        (bestLocation._1, bestLocation._2, 1.0, get_is_high_confidence_from_cluster_score(1.0))
      } else {
        (bestLocation._1, bestLocation._2, 0.0, get_is_high_confidence_from_cluster_score(0.0))
      }
    }
  }

  def get_cluster_centroid_lat_lng(cluster: List[(Double, Double, Double, Long)]): (Double, Double) = {
    val threshold_list :List[Int] = List(100, 75, 50, 25)
    val centroid_list = new mutable.ListBuffer[((Double, Double), Int)]()
    Logger.log(this.getClass, INFO, BaseSLog(s"Calculating centroid for selected cluster: $cluster"))
    for(threshold <- threshold_list){
      val centroid: ((Double, Double), Int) = find_cluster_centroid(cluster, threshold)
      if(centroid != null) {
        centroid_list.append(centroid)
      }
    }
    centroid_list.last._1
  }

  def find_cluster_centroid(cluster: List[(Double, Double, Double, Long)], threshold: Int): ((Double, Double), Int) ={
    var filtered_pings = cluster.filter(x => x._3 <=100)
    var centroid: (Double, Double) = null
    breakable {
      while (filtered_pings.size > 3) {
        val c_lat = filtered_pings.map(_._1).sum
        val c_lng = filtered_pings.map(_._2).sum
        centroid = (c_lat / filtered_pings.size, c_lng / filtered_pings.size)
        val distance_list = filtered_pings.map(x => (x, vincenty_distance(centroid._1, centroid._2, x._1, x._2)))
        val sorted_distance_list = distance_list.sortBy(_._2)
        val avg = sorted_distance_list.map(_._2).sum / sorted_distance_list.size
          if (avg > threshold) {
            val outliers = Math.max(filtered_pings.size * 0.1, 1)
            val new_sorted_distance_list = sorted_distance_list.slice(0, filtered_pings.size - outliers.toInt)
            filtered_pings = new_sorted_distance_list.map(x => x._1)
          } else
            break()
      }
    }
    (centroid, threshold)
  }
}