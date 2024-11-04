package com.flipkart.service

import java.util
import java.util.Calendar

import com.datastax.driver.core.SimpleStatement
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import com.flipkart.core.GeoTagPayload
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model.{SmartLookupModel, GeoTagCassandraModel}
import com.flipkart.utils.logging.Logger
import com.flipkart.utils.{JsonUtility, Utility, HttpClient}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import com.flipkart.utils.Geohash


import com.datastax.spark.connector._

import scala.collection.immutable.ListSet
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._


/**
 * Created by vikas.gargay on 24/10/16.
 */
object SmartAddressBuildService {

  var reverse_geocode_url = "http://10.85.50.71/api/v1/reverse-geocode"
  //var parse_poi_url = "http://fms-sage-app.vip.nm.flipkart.com:8888/parse_poi"
  var parse_poi_url = "http://ekl-routes-2.stage.nm.flipkart.com:8888/parse_poi"
  //var parse_poi_url = "http://localhost:8888/parse_poi"
  var mmi_docType_map = Map[String,Map[String,String]](
    "S_Sublocality_region" -> Map[String,String]("MMI_ID"->"SUBL_ID", "NAME"->"SSLC_NAME"),
    "Sublocality_region" -> Map[String,String]("MMI_ID"->"SUBL_ID", "NAME" -> "SUBL_NAME"),
    "Locality_region" -> Map[String,String]( "MMI_ID" ->"LOC_ID", "NAME"->"LOC_NAME" ),
    "City_region" -> Map[String,String]( "MMI_ID" ->"CITY_ID", "NAME"->"CITY_NAME" ),
    "Subcity_region" -> Map[String,String]( "MMI_ID" ->"SCITY_ID", "NAME"->"SCITY_NAME" ),
    "District_region" -> Map[String,String]( "MMI_ID" ->"DST_ID", "NAME"->"DST_NAME" ),
    "Subdistrict_region" -> Map[String,String]( "MMI_ID" ->"SDB_ID", "NAME"->"SDB_NAME" ),
    "State_region" -> Map[String,String]( "MMI_ID" ->"STT_ID", "NAME"->"STT_NAME" ),
    "Admin_Country_region" -> Map[String,String]( "MMI_ID" ->"CNT_ID", "NAME"->"CNT_NAME" )
  )
  lazy val CASSANDRA_KEY_SPACE = "compass"
  lazy val MINED_POI_TABLE = "mined_poi_v1"
  lazy val SMART_LOOKUP_TABLE = "smart_lookup_v1"

  val ACCURACY_THRESHOLD = 300
  val BOTTOM_LEFT = (6.7471, 68.032318)
  val TOP_RIGHT = (36.261688, 97.403023)


  // entry point
  def saveDataToCassandra(repartition:Boolean, numPartitions:Int, data_rdd: DStream[(String,String)], zkProperties:Map[String, String])(
    implicit
    rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
    var data_stream:DStream[(String, String)] = null
    if (repartition) {
      data_stream = data_rdd.repartition(numPartitions)
    }
    else{
      data_stream = data_rdd
    }
    data_stream.foreachRDD(rdd=> {
      //saveDataToCassandra(numPartitions,rdd,zkProperties)
      processGeoTagForPoiMining(rdd, zkProperties)
    })
  }

  def saveDataToCassandra(numPartitions:Int, rdd: RDD[(String,String)], zkProperties:Map[String, String])
                         (implicit rwf: RowWriterFactory[GeoTagCassandraModel]): Unit = {
    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val GEOTAG_TABLE = "geotag"
    try {
      implicit lazy val formats = org.json4s.DefaultFormats
      //parse the request and convert to cassandra model
      val geoTagStreamMap = rdd
        .map(x =>JsonUtility.deserialize[GeoTagPayload](x._2).convertToCassandraModel())
      //save to cassandra
      geoTagStreamMap.saveToCassandra(CASSANDRA_KEY_SPACE, GEOTAG_TABLE, GeoTagCassandraModel.getColumns)
      //commit the offsets once everything is done
      ZookeeperManager.updateOffsetsinZk(zkProperties, rdd)
    }
    catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog(s"Exceptions in saving to cassandra/updating offsets in zk: " + e.getMessage ,e))
        throw e;
    }
  }

  def call_reverse_geocode(lat:Double, lng:Double): JsonNode ={
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    headers.put("referer","http://geo-internal.flipkart.com/")
    val reverse_geoCoderParams = new java.util.HashMap[String,String]()
    reverse_geoCoderParams.put("point", lat.toString+","+lng.toString)
    reverse_geoCoderParams.put("key", "c2e7233f-5603-43e4-94da-cb9a926fec6f")
    val response = HttpClient.INSTANCE.executeGet(reverse_geocode_url,reverse_geoCoderParams, headers)
    if (response.getStatusCode != 200) {
      Logger.log(this.getClass, ERROR, BaseSLog("[reverse_geocode] Error, Returned status code " + response.getStatusCode))
      null
    }else {
      val mapper = new ObjectMapper()
      val root = mapper.readTree(response.getResponseBody)
      (root)
    }
  }

  def reverse_geocode(lat:Double, lng:Double): String = {
    var reverse_geocode = ""
    try {
      var resp = call_reverse_geocode(lat, lng)
      if (resp != null && resp.get("results") != null && resp.get("results").size() > 0) {
        reverse_geocode = resp.get("results").get(0).toString
      } else {
        reverse_geocode = ""
      }
    } catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("[reverse_geocode] Error, Returned status code " + e))
        reverse_geocode = ""
    }finally {
      return reverse_geocode
    }
    return reverse_geocode
  }

  def tokenize(addrFull : String): List[String] = {
    val addressClean = addrFull.replaceAll("[^a-z ]", " ");

    var tokens = new ListBuffer[String]

    val unaryTokens = addressClean.split("\\s+")

    for (i <- 0 to unaryTokens.length - 2){
      var newToken = unaryTokens(i) + " " + unaryTokens(i+1)
      tokens += newToken
    }

    for (i <- 0 to unaryTokens.length - 3){
      var newToken = unaryTokens(i) + " " + unaryTokens(i+1) + " " + unaryTokens(i+2)
      tokens += newToken
    }

    return tokens.toList
  }


  def getSmartTokens(cassandraConnector: CassandraConnector, key:(String,String,String)): SmartLookupModel = {
    var tripTrackerIds = new ListBuffer[String]()
    val resultSetRow = Utility.queryCassandra(cassandraConnector,
      new SimpleStatement("SELECT * FROM compass.smart_lookup_v1 WHERE mmi_id = ? and doc_type = ? and ngram = ?",
        key._1,key._2,key._3)).one()
    if(null != resultSetRow){
      //var points:ListBuffer[String] = ListBuffer()
      //var p = resultSetRow.getObject("points").asInstanceOf[java.util.LinkedHashSet[String]]
      //for(p1 <- p){
      //  points.+=(p1)
      //}
      return new SmartLookupModel(
        resultSetRow.getString("mmi_id"),
        resultSetRow.getString("doc_type"),
        resultSetRow.getString("ngram"),
        resultSetRow.getObject("geohashes").asInstanceOf[java.util.LinkedHashSet[String]].toList
      )
    }
    return null
  }


  // tokenize and the save to cassandra
  def tokenizeAndSave(cassandraConnector:CassandraConnector,reverse_geocode:String,lat_lng:(Double,Double),addr:(String,String)) : List[SmartLookupModel] ={
    var mmi_id:String = null
    var doc_type:String = null
    var poi_names:ArrayNode = null

    var smartLookList : ListBuffer[SmartLookupModel] = new ListBuffer[SmartLookupModel]

    try {
      val mapper = new ObjectMapper()
      if(reverse_geocode.length > 0){
        val root = mapper.readTree(reverse_geocode)
        val address_components = root.get("address_components").asInstanceOf[ArrayNode]

        for( address_component_idx <- 0 to address_components.size()-1){
          if(mmi_id==null && address_components.get(address_component_idx).get("DOCTYPE")!=null) {
            doc_type = address_components.get(address_component_idx).get("DOCTYPE").asText()
            if ("S_Sublocality_region" == doc_type
              || "Sublocality_region" == doc_type
              || "Locality_region" == doc_type
              || "City_region" == doc_type ) {
              mmi_id = address_components.get(address_component_idx).get("MMI_ID").asText()
              doc_type = address_components.get(address_component_idx).get("DOCTYPE").asText()
            }
          }
        }

        if(mmi_id != null && doc_type != null) {
          // have the mmi id and doc type at this point
          // should tokenize and save at this point ( also filter )

          val point_str: String = (lat_lng._1.toString + "," + lat_lng._2.toString)
          val encodedLocation = Geohash.encode(lat_lng._1, lat_lng._2, 7) // 7 is for 150*150 length

          // old code useful stubs:
          //
          // getMinedPoi(cassandraConnector,(doc_type, mmi_id, poi_name))
          // add to list
          // minedPoiModels.+=(minedPoiModel)
          // poi_names = parse_poi(addr._1, addr._2)

          // first tokenize the address
          val addressFull = (addr._1 + ' ' + addr._2).toLowerCase
          val tokens = tokenize(addressFull)

          for (token <- tokens){
            // fetch from cassandra
            // append/add to list/set
            // save back to cassandra

            var smartLookupModel = getSmartTokens(cassandraConnector, (mmi_id, doc_type, token))

            if (smartLookupModel == null){
              smartLookupModel = new SmartLookupModel(
                mmi_id,
                doc_type,
                token,
                new java.util.LinkedHashSet[String].toList
              )
            }

            if (! smartLookupModel.geohashes.contains(encodedLocation) ){
              var geohashesBuffer = smartLookupModel.geohashes.to[ListBuffer]
              geohashesBuffer +=  encodedLocation

              smartLookupModel.geohashes = geohashesBuffer.toList
            }

            smartLookList += smartLookupModel

          }

        }
      }
    }catch {
      case e : Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("[build_smart_lookup] Error, Returned status code " + e.getMessage,e))
    }finally {
    }
    smartLookList.toList
  }


  def processGeoTagForPoiMining(data_rdd: RDD[(String,String)], zkProperties:Map[String, String]): Unit = {
    try {
      val conf = data_rdd.sparkContext.getConf
      val cassandraConnector = CassandraConnector(conf)

      data_rdd.take(1).foreach(x => {
        Logger.log(this.getClass, INFO, BaseSLog("The data is : " + x._2))
      })

      var payloads = JsonUtility.deserialize[GeoTagPayload](data_rdd.map(_._2))
      payloads = payloads.keyBy(x => x.device_id).partitionBy(new HashPartitioner(60)).map(_._2)


      val filteredPayloads = payloads.filter(payload => {

        if (payload.address.length > 0 ) {
          val addr1List = payload.address.filter(x => x.get("addr1") != None)
          val addr2List = payload.address.filter(x => x.get("addr2") != None)


          if (payload.attributes.contains("accuracy_level") && addr1List.length > 0 && addr2List.length > 0) {
            val accuracyLevel: Int = payload.attributes("accuracy_level").toFloat.toInt
            var validVerificationCode = false

            if ( payload.attributes.contains("delivery_verification_code_type") ){
              val verificationCode = payload.attributes("delivery_verification_code_type")
              if (!verificationCode.equalsIgnoreCase("invalid_id")){
                validVerificationCode = true
              }
            }
            if ( validVerificationCode && accuracyLevel < ACCURACY_THRESHOLD &&
              ( payload.latitude > BOTTOM_LEFT._1 && payload.latitude < TOP_RIGHT._1) &&
              ( payload.longitude > BOTTOM_LEFT._2 && payload.longitude < TOP_RIGHT._2) &&
              !( payload.latitude.toFloat.toInt == 0 && payload.longitude.toFloat.toInt == 0)){
              true
            }else{
              false
            }
          }else{
            false
          }
        }else{
          false
        }
      })

      filteredPayloads.take(1).foreach(x => {
        Logger.log(this.getClass, INFO, BaseSLog("The first filtered data is : " + x))
      })

      val smartLookupRdd = filteredPayloads.flatMap(payload => {
        val reverse_geocode_resp: String = reverse_geocode(payload.latitude, payload.longitude)

        //Logger.log(this.getClass, INFO, BaseSLog("--- Payload.address : " + payload.address))

            tokenizeAndSave(cassandraConnector, reverse_geocode_resp, (payload.latitude, payload.longitude),
              (payload.address.filter(x => x.get("addr1") != None)(0).get("addr1").toString,
                payload.address.filter(x => x.get("addr2") != None)(0).get("addr2").toString))

      })
      smartLookupRdd.saveToCassandra(CASSANDRA_KEY_SPACE, SMART_LOOKUP_TABLE, SmartLookupModel.getColumns)
    } catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while processing RDD : " + e.getMessage, e))
    } finally {
      if (zkProperties != null) {
        ZookeeperManager.updateOffsetsinZk(zkProperties, data_rdd)
      }
    }
  }

}
