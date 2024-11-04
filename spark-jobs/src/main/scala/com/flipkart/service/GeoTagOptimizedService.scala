package com.flipkart.service


import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Calendar
import com.datastax.spark.connector._
import com.flipkart.config.Config
import com.flipkart.model.GeoTagCassandraModel
import com.flipkart.utils.BestLatLngCalculator
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.commons.lang.StringUtils
import org.apache.logging.log4j.Level.INFO
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable



object GeoTagOptimizedService {

  def get_accuracy_level(vec: List[UDTValue]): Double = {

    var accuracy_level = 0.0

    for (i <- vec.indices){
      if (vec(i).getString("key") == "accuracy_level"){
        accuracy_level = vec(i).getDouble("value")
      }
    }

    accuracy_level
  }

  def get_agent_id(vec: List[UDTValue]): String = {
    var agent_id = ""
    for (i <- vec.indices){
      if (vec(i).getString("key") == "agent_id"){
        agent_id = vec(i).getString("value")
      }
    }

    agent_id
  }

  def validate_attributes(vec: Vector[UDTValue]): Vector[UDTValue] = {

    var validated  = new mutable.ListBuffer[UDTValue]()
    for (i <- vec.indices){
        if (vec(i).getDoubleOption("lat").isDefined && vec(i).getDoubleOption("lng").isDefined){
            validated+=vec(i)
        }
    }
    validated.toVector
  }

  def get_ist_timestamp(time_stamp: String): BigInt = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssZ")
    val dt = LocalDateTime.parse(time_stamp, formatter).atZone(ZoneOffset.ofHoursMinutes(5,30)).toInstant
    dt.toEpochMilli
  }

  def saveDataToCassandraForLookup(geoTagStreamMap: RDD[GeoTagCassandraModel], ssc: StreamingContext, clusterScoreConfig:Map[String, String],
                                   CASSANDRA_KEY_SPACE: String, GEOTAG_OPTIMIZED_LOOKUP_TABLE: String,CASSANDRA_GEOTAG_OPT_READ_KEYSPACE:String): Unit ={

    Logger.log(this.getClass, INFO, BaseSLog(s"Filtering Geotag RDD"))
    val filtered_geoTag_rdd = get_filtered_rdd_from_geoTag_cassandra_model(geoTagStreamMap, ssc,
      CASSANDRA_GEOTAG_OPT_READ_KEYSPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE)

    val filtered_deduped_rdd = filtered_geoTag_rdd.map(x=> (x._1, deduplicateList(x._2)))

    Logger.log(this.getClass, INFO, BaseSLog(s"Getting best lat long"))
    val best_location_rdd = filtered_deduped_rdd.map(x => (x._1, BestLatLngCalculator.get_best_lat_lng(x._1, x._2), x._2))
    Logger.log(this.getClass, INFO, BaseSLog(s"Getting best lat long from cluster scoring method"))
    val best_location_with_cluster_score_rdd = best_location_rdd.map(x => (x._1, x._2, BestLatLngCalculator.get_best_lat_lng_with_cluster_score_v2(x._1, x._3, clusterScoreConfig), x._3))
    val geoTag_optimized_cassandra_model_rdd = best_location_with_cluster_score_rdd
      .map(x => (x._1, x._2._1, x._2._2, x._4.map(y=>(y._2,y._3,y._4,y._1)), x._4.find(y=> !y._6.equals("198")).getOrElse(x._4.last)._6, x._4.last._5, x._3._1, x._3._2, x._3._3, x._4.last._7, x._4.last._8))

    Logger.log(this.getClass, INFO, BaseSLog(s"Saving Records TO Geotag_optimized_lookup table"))

    geoTag_optimized_cassandra_model_rdd.saveToCassandra(CASSANDRA_KEY_SPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE,
      SomeColumns("addr_hash", "lat", "lng", "attributes", "precision", "addr", "cluster_lat", "cluster_lng", "cluster_score", "address_id", "account_id"))


    /*Logger.log(this.getClass, INFO, BaseSLog("Calling geocode-score service"))

    geoTag_optimized_cassandra_model_rdd.foreach(x => GeocodeScoreService.ingest_geocode_score_in_varadhi(
      GeocodeScorePayload(null, null, null, x._1,
        KeyValuePairGenerator.UDTListToAddressForLookup(x._6), "DELIVERY", null)))*/


    Logger.log(this.getClass, INFO, BaseSLog(s"Push messages to Varadhi"))
    val filteredData=geoTag_optimized_cassandra_model_rdd.filter(x=> checkNullUDTValues(x._6))


    filteredData.foreachPartition(partition => {
      partition.foreach(x => {
        val payload = CacheRefreshService.getCacheRefreshPayload(x._6, x._7, x._8, x._9)
        Logger.log(this.getClass, INFO, BaseSLog(s"Payload: "))
        Logger.log(this.getClass, INFO, BaseSLog(payload.toString))
        CacheRefreshService.refresh_cache_through_varadhi(payload, x._1)
      }
      )
    }
    )

  }

  def checkNullUDTValues(udtList: List[UDTValue]) : Boolean={

    Logger.log(this.getClass, INFO, BaseSLog(udtList.toString()))
    if(udtList.isEmpty){
      return false
    }
    for (udtValue: UDTValue <- udtList) {
      if (udtValue != null && udtValue.getStringOption("key").isDefined
        && udtValue.getStringOption("value").isDefined) {
        lazy val key = udtValue.getString("key")
        lazy val value = udtValue.getString("value")
        if (StringUtils.isEmpty(key) || (key.equals("addr1") && StringUtils.isEmpty(value)) || key.equalsIgnoreCase("null")) {
          Logger.log(this.getClass, INFO, BaseSLog(s"Invalid Payload for VARADHI"))
          return false
        }
      }
      else{
        Logger.log(this.getClass, INFO, BaseSLog(s"Invalid Payload for VARADHI"))
        return false
      }
    }
    Logger.log(this.getClass, INFO, BaseSLog(s"Valid Payload for VARADHI "))
    true
  }


  def get_filtered_rdd_from_geoTag_cassandra_model
  (geoTagStreamMap: RDD[GeoTagCassandraModel], ssc: StreamingContext, CASSANDRA_KEY_SPACE: String,
   GEOTAG_OPTIMIZED_LOOKUP_TABLE: String): RDD[(String, List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])])] ={
    lazy val REFINED_DELIVERED_LOCATION_TABLE = "refined_delivered_data"
    val geoTagDeliveredStream = geoTagStreamMap.filter(x => (x._type.equals("DEL") || x._type.equals("PC")) && x.lat != 0 && x.lng != 0)

    //Check whether ingestion is enabled for refined_delivered_data table
    if(Config.getProperty("cassandra.refinedDeliveredLocationIngestion.enabled","false").toBoolean) {
      val refined_delivered_location_cassandra_model_rdd = geoTagDeliveredStream
        .map(x => (x.addr_hash, x.date, x.device_id, x.tag_id, x.lat, x.lng, get_ist_timestamp(x.time), get_accuracy_level(x.attributes), get_agent_id(x.attributes), false)).filter(x => x._8 > 0 && x._8 < 200)
      refined_delivered_location_cassandra_model_rdd.saveToCassandra(CASSANDRA_KEY_SPACE, REFINED_DELIVERED_LOCATION_TABLE,
        SomeColumns("addr_hash", "date", "device_id", "shipment_id", "delivered_lat", "delivered_lng", "timestamp", "accuracy", "agent_id", "is_refined"))
    }
    //removing TTL for now, will have to be added later as per discussions
    val geoTagStreamFiltered: RDD[(String, (String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String]))] = geoTagDeliveredStream
      .map(x =>
        (x.addr_hash, (x.time, x.lat, x.lng, get_accuracy_level(x.attributes), x.addr, "198", Option(x.address_id), Option(x.account_id))))
      .filter(x => x._2._4 > 0 && x._2._4 < 200)

    val geoTagStreamCombined = get_combined_rdd_for_addr_hash(geoTagStreamFiltered)

    val addr_hash_list: List[String] = geoTagStreamCombined.map(x => x._1).collect().toList
    val len = addr_hash_list.length
    Logger.log(this.getClass, INFO, BaseSLog(s"Will fetch $len records from geotag_optimized_lookup table"))

    val geoTag_optimized_expanded_rdd = get_expanded_rdd_from_optimized_table(
      ssc, addr_hash_list, CASSANDRA_KEY_SPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE)

    Logger.log(this.getClass, INFO, BaseSLog(s"Combining Records From Geotag and optimized"))
    val rdd_union = geoTagStreamCombined
      .union(geoTag_optimized_expanded_rdd)
      .reduceByKey((x,y) => x ::: y)
      .map(x => (x._1, x._2.sortBy(x => x._1)))
      .filter(x => x._2.nonEmpty)
    rdd_union

  }


  def get_combined_rdd_for_addr_hash(geoTagStreamFiltered: RDD[(String, (String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String]))]):
  RDD[(String, List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])])] ={

    Logger.log(this.getClass, INFO, BaseSLog(s"Combining RDD on Address Hash"))
    val geoTagStreamCombined = geoTagStreamFiltered.combineByKey(
      // createCombiner: add first value to a list
      (x: (String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])) => List(x),
      // mergeValue: add new value to existing .l list
      (acc: List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])], x) => x :: acc,
      // mergeCominber: combine the 2 lists
      (acc1: List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])],

       acc2: List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])]) => acc1 ::: acc2
    )

    geoTagStreamCombined
  }


  def get_expanded_rdd_from_optimized_table(ssc: StreamingContext, addr_hash_list: List[String],
                             CASSANDRA_KEY_SPACE:String, GEOTAG_OPTIMIZED_LOOKUP_TABLE:String):
                            RDD[(String, List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])])] ={

    Logger.log(this.getClass, INFO, BaseSLog(s"Fetching Records from Geotag_optimized_lookup"))
    val geoTag_optimized_rdd = ssc.sparkContext.cassandraTable(CASSANDRA_KEY_SPACE, GEOTAG_OPTIMIZED_LOOKUP_TABLE)
      .select("addr_hash", "attributes", "addr", "precision", "address_id", "account_id").where("addr_hash IN ?", addr_hash_list)

    val arr = geoTag_optimized_rdd.filter(x => x.getStringOption("precision").isEmpty).map(x => x.getString("addr_hash")).collect()
    if (arr != null && arr.nonEmpty) {
      Logger.log(this.getClass, INFO, BaseSLog(s"Address hash with null precision: ${arr(0)}"))
    }
    val geoTag_optimized_value_rdd = geoTag_optimized_rdd.map(x=>(x.getString("addr_hash"), validate_attributes(x.getList[UDTValue]("attributes")), x.getList[UDTValue]("addr"),
      x.getStringOption("precision").getOrElse("0"), x.getStringOption("address_id"), x.getStringOption("account_id") ))

    val geoTag_optimized_expanded_rdd = geoTag_optimized_value_rdd.map(x=>(x._1, x._2.toList.map(y=>
      ( if (y.getStringOption("time").isDefined)
        {y.getString("time")} else
        {new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance.getTime)},
        y.getDouble("lat"), y.getDouble("lng"),
        if (y.getDoubleOption("accuracy_level").isDefined)
        {y.getDouble("accuracy_level")} else {198},
        x._3.toList, x._4, x._5, x._6))))

    geoTag_optimized_rdd.unpersist()

    geoTag_optimized_expanded_rdd

  }

  def deduplicateList(vec: List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])]): List[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])] ={

    var locationSet = mutable.HashSet[(Double, Double, Double)]()

    var finalList = new mutable.ListBuffer[(String, Double, Double, Double, List[UDTValue], String, Option[String], Option[String])]()

    val initialLength = vec.length

    Logger.log(this.getClass, INFO, BaseSLog(s"Deduping List with Length : $initialLength"))

    for (i <- vec.indices){
      if(!locationSet.contains((vec(i)._2, vec(i)._3, vec(i)._4))){
        finalList+=vec(i)
          locationSet.+=((vec(i)._2, vec(i)._3, vec(i)._4))
      }
    }

    val finalLength = finalList.length

    Logger.log(this.getClass, INFO, BaseSLog(s"After Deduping Length : $finalLength"))

    if(finalLength > 100){
      finalList = finalList.takeRight(100)
    }

    finalList.toList
  }

}