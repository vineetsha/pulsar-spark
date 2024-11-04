package com.flipkart.service

import java.util.UUID

import com.datastax.spark.connector.UDTValue
import com.flipkart.config.Config
import com.flipkart.model.CacheRefreshPayload
import com.flipkart.utils.HttpClient
import com.flipkart.utils.logging.{BaseSLog, Logger}
import com.google.gson.{FieldNamingPolicy, Gson, GsonBuilder}
import org.apache.logging.log4j.Level.INFO

object CacheRefreshService {
  var configMap: Map[String, String] = Map[String, String]()
  configMap += "cacheRefreshVaradhiEndpoint" -> Config.getProperty("CacheRefresh.Varadhi.Endpoint", "http://10.24.0.208/queues/flip_geocode_cache_refresh_production/messages")
  configMap += "cacheRefreshEndpoint" -> Config.getProperty("CacheRefresh.Endpoint", "http://10.24.1.76/refresh-cache")
  var gson: Gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create
  def refresh_cache_through_varadhi(cacheRefreshPayload: CacheRefreshPayload, addressHash: String): Unit = {
    val msg_id = UUID.randomUUID().toString
    val group_id = addressHash
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    headers.put("X_RESTBUS_MESSAGE_ID",msg_id)
    headers.put("X_RESTBUS_GROUP_ID", group_id)
    headers.put("X_RESTBUS_HTTP_URI", configMap.apply("cacheRefreshEndpoint"))
    headers.put("X_RESTBUS_HTTP_METHOD", "PUT")
    Logger.log(this.getClass, INFO, BaseSLog(s"Calling varadhi with payload: $addressHash - ${CacheRefreshPayload.toString()}"))
    val response = HttpClient.INSTANCE.executePost(configMap.apply("cacheRefreshVaradhiEndpoint"), gson.toJson(cacheRefreshPayload), headers)
    Logger.log(this.getClass, INFO, BaseSLog(s"post response body: " + response.getResponseBody))
  }

  def getCacheRefreshPayload(udtList: List[UDTValue], updatedLat: Double, updatedLng: Double, clusterScore: Double):
  CacheRefreshPayload = {
    var addr1 = ""
    var addr2 = ""
    var city = ""
    var state = ""
    var pincode = ""
    var country = ""
    if(udtList != null){
      for (udtValue: UDTValue <- udtList) {
        if (udtValue != null && udtValue.getStringOption("key").isDefined
          && udtValue.getStringOption("value").isDefined) {
          var key = udtValue.getString("key")
          var value = udtValue.getString("value")
          if (key.equals("addr1")) {
            addr1 = value
          }
          if (key.equals("addr2")) {
            addr2 = value
          }
          if (key.equals("city")) {
            city = value
          }
          if (key.equals("state")) {
            state = value
          }
          if (key.equals("pincode")) {
            pincode = value
          }
          if (key.equals("country")) {
            country = value
          }
        }
      }
    }
    CacheRefreshPayload(null, null, addr1, addr2, city, state, pincode, country, updatedLat, updatedLng, clusterScore)
  }

}
