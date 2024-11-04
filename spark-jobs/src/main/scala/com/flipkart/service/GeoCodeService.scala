package com.flipkart.service

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, UUID}

import com.datastax.driver.core
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.flipkart.core.{EventHistoryPayload, GeoCodeValidationPayload, GeoTagPayload}
import com.flipkart.messaging.ZookeeperManager
import com.flipkart.model._
import com.flipkart.streaming.GeoTagStream
import com.flipkart.utils._
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.google.common.util.concurrent.ListenableFuture
import org.apache.logging.log4j.Level._
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.json4s._
import org.json4s.native.Json

import scala.collection.mutable.ListBuffer


object GeoCodeService {
  def computeGeoCodeDeviation(data_rdd: RDD[(String,String)], zkProperties:Map[String, String], configMap:Map[String, String] ) : Unit = {
    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val GEOCODE_DEVIATION_TABLE = "geocode_deviation"
    try {
        val geoTagStreamMap = data_rdd
          .map(x => JsonUtility.deserialize[GeoTagPayload](x._2))
          .map(geoTag=>{
          val (addr1,addr2,city,state,pincode,hubCOC) = callShipmentTrack(configMap.apply("fsdExternalEndPoint"),geoTag.tag_id)
          val geoCodeRsp = callGeoCoder(configMap.apply("geoCoderEndPoint"),addr1,addr2,city,state,pincode )
          val (geoCodedLat,geoCodedLng) = parseLatLng(geoCodeRsp)
          val hubId = callFacility(configMap.apply("facilityEndPoint"),hubCOC)
          val geoTagDeviationModel = GeoTagDeviationModel(hubId.toString, geoTag.date, geoTag.address_hash, geoTag.tag_id,
            geoTag.latitude,  geoTag.longitude,
            geoTag.attributes.apply("accuracy_level").toDouble,
            geoCodedLat, geoCodedLng, geoTag.address_full)
          Logger.log(this.getClass, INFO, BaseSLog("[GeoCodeService-saveDataToCassandra] Model: " + geoTagDeviationModel.toString))
          geoTagDeviationModel
        })
        geoTagStreamMap.saveToCassandra(CASSANDRA_KEY_SPACE, GEOCODE_DEVIATION_TABLE, GeoTagDeviationModel.getColumns)
    }catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while processing RDD : " +  e.getMessage, e))
    }finally {
      if(zkProperties != null) {
        ZookeeperManager.updateOffsetsinZk(zkProperties, data_rdd)
      }
    }
  }

  def callShipmentTrack(fsdExternalEndPoint:String, trackingId: String) : (String,String,String,String,String,String) ={
    var pincode ,hubCOC, addr1, addr2, city, state = ""
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    //call shipment tracking api
    val shipment_track_url = fsdExternalEndPoint + "/shipments/" + trackingId + "/track"
    Logger.log(this.getClass, INFO, BaseSLog("[shipment_tacking] url: " + shipment_track_url))
    val response = HttpClient.INSTANCE.executeGet(shipment_track_url, null, headers)
    //Logger.log(this.getClass, INFO, BaseSLog("[shipment_tacking]response body: " + response.getResponseBody))
    try {
      if (response.getStatusCode != 200) {
        Logger.log(this.getClass, ERROR, BaseSLog("[shipment_tacking] Error, Returned status code "
          + response.getStatusCode))
        //throw new RuntimeException
      } else {
        val mapper = new ObjectMapper()
        val root = mapper.readTree(response.getResponseBody)

        addr1 = root.get("customerModel").get("address").asText()
        addr2 = root.get("customerModel").get("address2").asText()
        city = root.get("customerModel").get("city").asText()
        state = root.get("customerModel").get("state").asText()
        hubCOC = root.get("hubInfo").get("cocCode").asText()
        pincode = root.get("customerModel").get("pincode").asText()
        Logger.log(this.getClass, INFO, BaseSLog("[shipment_tacking] :" + "addr1:" + addr1 + "addr2:" + addr2
          + "city:" + city + "state:" + state + "pincode:" + pincode + " hubCOC:" + hubCOC))
      }
    }catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while geocoding: " +  e.getMessage, e))
    }finally {
      (addr1, addr2, city, state, pincode, hubCOC)
    }
    (addr1, addr2, city, state, pincode, hubCOC)
  }

  def callGeoCoder(geoCoderEndPoint:String,addr1:String,addr2:String,city:String,state:String,pincode:String) : (JsonNode) ={
    var geoCodedLat = 0.0
    var geoCodedLng = 0.0
    val geoCoderUrl = geoCoderEndPoint + "/geoverify"
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    val geoCoderParams = new java.util.HashMap[String,String]()
    geoCoderParams.put("addr1", addr1)
    geoCoderParams.put("addr2", addr2)
    geoCoderParams.put("city", city)
    geoCoderParams.put("state", state)
    geoCoderParams.put("pincode", pincode)
    var root: JsonNode = null
    try {
      Logger.log(this.getClass, INFO, BaseSLog("[geocode] url: " + geoCoderUrl + " params : " + geoCoderParams))
      val response = HttpClient.INSTANCE.executeGet(geoCoderUrl, geoCoderParams, headers)
      //Logger.log(this.getClass, INFO, BaseSLog("[geocode]response body: " + response.getResponseBody))

      if (response.getStatusCode != 200) {
        Logger.log(this.getClass, ERROR, BaseSLog("[geocode] Error, Returned status code " + response.getStatusCode))
        (root)
      } else {
        val mapper = new ObjectMapper()
        root= mapper.readTree(response.getResponseBody)
        (root)
      }
    }catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while geocoding: " +  e.getMessage, e))
        (root)
    }finally {
      (root)
    }
  }

  def parseLatLng(geoCodeRsp: JsonNode ): (Double,Double) ={
    var geoCodedLat = 0.0
    var geoCodedLng = 0.0
    if(geoCodeRsp.get("results") != null){
      geoCodedLat = geoCodeRsp.get("results").get("geometry").get("location").get("lat").asDouble()
      geoCodedLng = geoCodeRsp.get("results").get("geometry").get("location").get("lng").asDouble()
      Logger.log(this.getClass, INFO, BaseSLog("[geocode] lat :" + geoCodedLat + " lng:" + geoCodedLng))
    }
    (geoCodedLat,geoCodedLng)
  }

  def callFacility(facilityEndPoint:String, hubCOC:String) : Long ={
    var hubId = 0L
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    val facilityUrl = facilityEndPoint + "/facilities"
    val facilityParams = new java.util.HashMap[String,String]()
    facilityParams.put("code", hubCOC)

    Logger.log(this.getClass, INFO, BaseSLog("[facility] url: " + facilityUrl + " params : " + facilityParams))
    val response = HttpClient.INSTANCE.executeGet(facilityUrl, facilityParams, headers)
    //Logger.log(this.getClass, INFO, BaseSLog("[facility]response body: " + response.getResponseBody))

    if (response.getStatusCode != 200) {
      Logger.log(this.getClass, ERROR, BaseSLog("[facility] Error, Returned status code " + response.getStatusCode))
    }else {
      val mapper = new ObjectMapper()
      val root = mapper.readTree(response.getResponseBody)
      hubId = root.get(0).get("id").asLong()
      Logger.log(this.getClass, INFO, BaseSLog("[facility] geoLocation: " + hubId))
    }
    hubId
  }

  def manuallyGeoCoded( cassandraConnector: CassandraConnector, payload: GeoCodeValidationPayload): Boolean = {
    var count = 0
    cassandraConnector.withSessionDo(session => {
       count = session.execute(new SimpleStatement("select * from compass.geocode_validation WHERE addr_id = ? ", payload.address_id )).all().size()
    })
    if (count == 0) {
      Logger.log(this.getClass, INFO, BaseSLog("addr_id not present" + payload.address_id))
      true
    } else {
      Logger.log(this.getClass, INFO, BaseSLog("addr_id present = " + payload.address_id))
      false
    }
  }

  def postToQueue(configMap: Map[String, String], payload:String, httpUrl:String, msgQUrl:String) : Boolean = {
    Logger.log(this.getClass, INFO, BaseSLog("[GeoCodeService-postToQueue] " +" msgQUrl:" + msgQUrl +
      " httpUrl:" + httpUrl + " payload: " + payload))
    val msg_id = UUID.randomUUID().toString
    val group_id = UUID.randomUUID().toString
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    headers.put("X_RESTBUS_MESSAGE_ID",msg_id)
    headers.put("X_RESTBUS_GROUP_ID", group_id)
    headers.put("X_RESTBUS_HTTP_URI", httpUrl)
    headers.put("X_RESTBUS_HTTP_METHOD", "POST")
    val response = HttpClient.INSTANCE.executePost(msgQUrl, payload, headers)
    Logger.log(this.getClass, INFO, BaseSLog("post response headers: " + response.getStatusCode + " " + response.getHeaders))
    Logger.log(this.getClass, INFO, BaseSLog("post response body: " + response.getResponseBody))
    if (response.getStatusCode != 200) {
      Logger.log(this.getClass, ERROR, BaseSLog("[GeoCodeService-postToQueue] Error : Returned status code " + response.getStatusCode))
      //throw new RuntimeException
    }
    true
  }

  def buildUserGeoCodingReg(payload:GeoCodeValidationPayload): String = {
    val request = JsonUtility.serialize(Map("address_id" -> payload.address_id))
    request
  }

  def buildMMiGeoCodingReq(): String = {
    val request = new String()

    request
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

  def filterGeoCodesForVerification(data_rdd: RDD[(String,String)], zkProperties:Map[String, String],
                                    configMap:Map[String, String]): Unit = {
    var data_stream:DStream[(String, String)] = null
    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val GEOCODE_VALIDATION_TABLE = "geocode_validation"

    try {
        val conf = data_rdd.sparkContext.getConf
        val cassandraConnector = CassandraConnector(conf)
        val streamMap = data_rdd
          .map(x => JsonUtility.deserialize[GeoCodeValidationPayload](x._2))
          .filter(payload => {manuallyGeoCoded(cassandraConnector, payload)})
          .map(payload=>{
              val addressStr = payload.deliveryCustomerAddress1 + " " + payload.deliveryCustomerAddress2 + " " + payload.deliveryCustomerCity + " " + payload.deliveryCustomerPincode
              val geoCodeRsp = callGeoCoder(configMap.apply("geoCoderEndPoint"), payload.deliveryCustomerAddress1,
              payload.deliveryCustomerAddress2,payload.deliveryCustomerCity,payload.deliveryCustomerState,payload.deliveryCustomerPincode)
              val (geoCodedLat,geoCodedLng) = parseLatLng(geoCodeRsp)
              val precise = geoCodeRsp.get("result").get("precise").asBoolean()
              val attributeMap = scala.collection.mutable.Map[String, String]()
              attributeMap.put("city",payload.deliveryCustomerCity)
              attributeMap.put("state",payload.deliveryCustomerState)
              attributeMap.put("pincode",payload.deliveryCustomerPincode)
              val addressComponentsSize = geoCodeRsp.get("result").get("address_components").size()
              for(i <- 0 to addressComponentsSize-1){
                val addressComponent = geoCodeRsp.get("result").get("address_components").get(i)
                val docType = addressComponent.get("DOCTYPE").asText()
                val docTypeVal = addressComponent.get("NAME").asText()
                Logger.log(this.getClass, INFO, BaseSLog("[GeoCodeService-filterGeoCodesForVerification] " + docType +":" + docTypeVal ))
                attributeMap.put(docType,docTypeVal)
              }
              if(precise == false){
                postToQueue(configMap, buildUserGeoCodingReg(payload), configMap.apply("userGeoCodingServiceUrl"), configMap.apply("userGeoCodingMsgQUrl"))
                //postToQueue(configMap, buildMMiGeoCodingReq(), configMap.apply("mmiGeoCodingServiceUrl"), configMap.apply("mmiGeoCodingMsgQUrl"))
              }else{
              }
              val geoCodeValidationModel = GeoCodeValidationModel(payload.address_id,
                addressStr,
                Some(geoCodedLat),
                Some(geoCodedLng),
                null,
                null,
                null,
                null,
                KeyValuePairGenerator.fromMapToUDTList(attributeMap.toMap))
              Logger.log(this.getClass, INFO, BaseSLog("[GeoCodeService-filterGeoCodesForVerification] Model: " + geoCodeValidationModel.toString))
              geoCodeValidationModel
      })
        streamMap.saveToCassandra(CASSANDRA_KEY_SPACE, GEOCODE_VALIDATION_TABLE, GeoCodeValidationModel.getColumns)
    }catch {
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while processing RDD : " +  e.getMessage, e))
    }finally {
      if(zkProperties!= null) {
        ZookeeperManager.updateOffsetsinZk(zkProperties, data_rdd)
      }
    }
  }

  def queryCassandra( cassandraConnector: CassandraConnector, queryStatement:SimpleStatement ): core.ResultSet = {
    Logger.log(this.getClass, INFO, BaseSLog("[GeoCodeService-queryCassandra] queryStatement" + queryStatement.getQueryString()))
    cassandraConnector.withSessionDo(session => {
      return session.execute(queryStatement)
    })
  }

  def presentIncache(cassandraConnector: CassandraConnector, addr_hash: String):Boolean={
    val count = queryCassandra(cassandraConnector,
      new SimpleStatement("SELECT addr_hash FROM compass.geocode_cache WHERE addr_hash = ?", addr_hash)).all().size()
    if (count == 0) {
      Logger.log(this.getClass, INFO, BaseSLog("geocode not present" + addr_hash))
      false
    } else {
      Logger.log(this.getClass, INFO, BaseSLog("geocode present = " + addr_hash))
      true
    }
  }


  def callPincodeInfo(pincodeInfoEndPoint:String, pincode:String) : (JsonNode) ={
    val pincodeInfoUrl = pincodeInfoEndPoint + "/get_pincode_info"
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    val pincodeInfoParams = new java.util.HashMap[String,String]()
    pincodeInfoParams.put("pincode", pincode)
    pincodeInfoParams.put("doctypes", "State_region")
    pincodeInfoParams.put("docfields", "NAME")
    var root: JsonNode = null
    try {
      Logger.log(this.getClass, INFO, BaseSLog("[pincode_info] url: " + pincodeInfoUrl + " params : " + pincodeInfoParams))
      val response = HttpClient.INSTANCE.executeGet(pincodeInfoUrl, pincodeInfoParams, headers)
      if (response.getStatusCode != 200) {
        Logger.log(this.getClass, ERROR, BaseSLog("[pincode_info] Error, Returned status code " + response.getStatusCode))
        (root)
      } else {
        val mapper = new ObjectMapper()
        root= mapper.readTree(response.getResponseBody)
        (root)
      }
    }catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while geocoding: " +  e.getMessage, e))
        (root)
    }finally {
      (root)
    }
  }


  def checkDeliveryLookup(spyglassEndPoint:String, payload:String) : (JsonNode) ={

    Logger.log(this.getClass, ERROR, BaseSLog("inside checkDeliveryLookup function "))

    val spyglassURL = spyglassEndPoint + "/geotag/from-address"
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    var root: JsonNode = null
    try {

      Logger.log(this.getClass, INFO, BaseSLog("[spyglass] url: " + spyglassURL + " params : " + payload))
      val response = HttpClient.INSTANCE.executePost(spyglassURL, payload, headers)

      if (response.getStatusCode != 200) {
        Logger.log(this.getClass, ERROR, BaseSLog("[spyglass] Error, Returned status code " + response.getStatusCode))
        (root)
      } else {
        val mapper = new ObjectMapper()
        root= mapper.readTree(response.getResponseBody)
        (root)
      }
    }catch{
      case e: Exception =>
        Logger.log(this.getClass, ERROR, BaseSLog("Exceptions while delivery lookup: " +  e.getMessage, e))
        (root)
    }finally {
      (root)
    }
  }

  // TODO: remove this as its serving no use than simply returning false
  def presentInDeliveryLookup(spyglassEndPoint:String, addr1:String, addr2:String, city:String,
                              state:String, pincode:String): Boolean = {

    Logger.log(this.getClass, INFO, BaseSLog("inside present in delivery function "))

    var addr = Map("addr1" -> addr1, "addr2" -> addr2, "city" -> city, "state" -> state,
      "pincode" -> pincode, "country" -> "India")

    var address_map = Map("address" -> List(addr))
//    val payload = JsonUtility.serialize(List(address_map))

    val payload = Json(DefaultFormats).write(List(address_map))


    Logger.log(this.getClass, INFO, BaseSLog("payload is " + payload))

    false
  }


  def presentInGeotag(cassandraConnector: CassandraConnector, addr_text: String):Boolean={
    var formatted_addr_text = addr_text + " " + "India"
    val addr_hash = Utility.md5String(addr_text)
    val count = queryCassandra(cassandraConnector,
      new SimpleStatement("SELECT addr_hash FROM compass.geotag WHERE addr_hash = ?", addr_hash)).all().size()
    if (count == 0) {
      Logger.log(this.getClass, INFO, BaseSLog("delivery latlng not present" + addr_hash))
      false
    } else {
      Logger.log(this.getClass, INFO, BaseSLog("delivery latlng present = " + addr_hash))
      true
    }
  }


  def buildGeocodeCache(data_rdd: RDD[(String,String)], zkProperties:Map[String, String],
                               configMap:Map[String, String]): Unit  = {
    lazy val CASSANDRA_KEY_SPACE = "compass"
    lazy val GEOCODE_CACHE_TABLE = "geocode_cache"
    try {
      val conf = data_rdd.sparkContext.getConf
      val cassandraConnector = CassandraConnector(conf)
      var pincodes = queryCassandra(cassandraConnector,
        new SimpleStatement("select pincodes from compass.hubid_clusters")).all()

      val pin_list = new ListBuffer[String]()

      for( i <- 0 to pincodes.size-1){

        val pin = pincodes.get(i).getString("pincodes").split(",")
        pin_list ++= pin
      }
      val pincode_list = pin_list.toList

      var payloads = JsonUtility.deserialize[ShipmentBigfootEntry](data_rdd.map(_._2))

      payloads = payloads.keyBy(x => x.entity.data.vendor_tracking_id).partitionBy(new HashPartitioner(5)).map(x => x._2)

      payloads.map(payload => {
        var geocodeCacheModel : GeocodeCacheModel = GeocodeCacheModel(null,null,null,null,null,null)
        if (payload.entity.data.status.compareToIgnoreCase("expected")==0
          && payload.entity.data.destination_address!= null
          && pincode_list.contains(payload.entity.data.destination_address.pincode)) {
          val (addr1, addr2, city, state, pincode, hubCOC) = callShipmentTrack(configMap.apply("fsdExternalEndPoint"), payload.entity.data.vendor_tracking_id)
          val addr_text = addr1 + " " + addr2 + " " + city + " " + state + " " + pincode
          val address_hash = Utility.md5String(addr_text)
          if (!(presentInGeotag(cassandraConnector, addr_text))){
            if (!presentIncache(cassandraConnector, address_hash)) {
              if(addr1!="" && city!=""&& city!="" && state!="" && pincode!="") {
                val pincodeInfoRsp = callPincodeInfo(configMap.apply("pincodeInfoEndPoint"), pincode)
                val correctedState = pincodeInfoRsp.get("pincode_info").get("State_region").get("docs").get(0).get("NAME").toString
                val geoCodeRsp = callGeoCoder(configMap.apply("geoCoderEndPoint"), addr1, addr2, city, correctedState, pincode)
                val addr_udt = GeoTagStream.convertAddress(List(Map("addr1" -> addr1), Map("addr2" -> addr2), Map("city" -> city), Map("state" -> state), Map("pincode" -> pincode)))
                if (payload != null && payload.entity != null && payload.entity.data != null && geoCodeRsp != null) {
                  geocodeCacheModel = GeocodeCacheModel(address_hash, payload.entity.data.vendor_tracking_id, addr_text, addr_udt, geoCodeRsp.toString, Calendar.getInstance().getTime())
                }
                Logger.log(this.getClass, INFO, BaseSLog("[GeoCodeService-buildGeocodeCache] Model: " + geocodeCacheModel.toString))
              }
            }
          }
        }
        geocodeCacheModel
      })
        .filter(x => x.addr_hash != null && x.tracking_id!=null && x.addr !=null && x.create_time !=null && x.geocode_result!=null && x.addr_text!=null)
        .saveToCassandra(CASSANDRA_KEY_SPACE,GEOCODE_CACHE_TABLE, GeocodeCacheModel.getColumns)
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
