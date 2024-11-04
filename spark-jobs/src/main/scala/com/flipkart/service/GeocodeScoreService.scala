package com.flipkart.service

import java.util.UUID

import com.flipkart.config.Config
import com.flipkart.model.GeocodeScorePayload
import com.flipkart.utils.HttpClient
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level.INFO
import com.google.gson.{FieldNamingPolicy, Gson, GsonBuilder};

object GeocodeScoreService {
  var configMap: Map[String, String] = Map[String, String]()
  configMap += "geocodeScoreVaradhiEndpoint" -> Config.getProperty("GeocodeScore.Varadhi.Endpoint", "http://10.24.0.208/queues/geocode-score-production/messages")
  configMap += "geocodeScoreEndpoint" -> Config.getProperty("GeocodeScore.Endpoint", "http://10.24.1.76/update-geocode-score")
  var gson: Gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create
  def ingest_geocode_score_in_varadhi(geocodeScorePayload: GeocodeScorePayload): Unit = {
    val msg_id = UUID.randomUUID().toString
    val group_id = geocodeScorePayload.addressHash
    val headers = new java.util.HashMap[String, String]()
    headers.put("Content-Type", "application/json")
    headers.put("X_RESTBUS_MESSAGE_ID",msg_id)
    headers.put("X_RESTBUS_GROUP_ID", group_id)
    headers.put("X_RESTBUS_HTTP_URI", configMap.apply("geocodeScoreEndpoint"))
    headers.put("X_RESTBUS_HTTP_METHOD", "PUT")
    val response = HttpClient.INSTANCE.executePost(configMap.apply("geocodeScoreVaradhiEndpoint"), gson.toJson(geocodeScorePayload), headers)
    Logger.log(this.getClass, INFO, BaseSLog(s"post response body: " + response.getResponseBody))
  }

}
