package com.flipkart.utils

import com.datastax.driver.core.{ResultSet, SimpleStatement}
import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.flipkart.config.Config
import com.flipkart.utils.Constant.{AUTHN_URL, CLIENT_ID, CLIENT_SECRET}
import com.flipkart.utils.logging.{BaseSLog, Logger}
import org.apache.logging.log4j.Level._
import org.apache.pulsar.client.api.Authentication
import org.apache.pulsar.client.api.url.DataURLStreamHandler
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2
import org.apache.spark.streaming.pulsar.SparkPulsarMessage
import org.json4s.DefaultFormats
import org.json4s.native.Json

import java.net.URL
import java.security.MessageDigest
import java.util.Base64


object Utility {

  final val CompanyEstTime = "2007-01-01T23:59:59.000+05:30"
  final val defaultRetries: Int = 3
  final val EMPTY_STRING = ""
  final val TYPE = "type"
  final val CLIENT_CREDENTIALS = "client_credentials"
  final val AUDIENCE = "authn"


  def getEnvironmentVariable(env: String): Option[String] = {
    val environment = System.getenv(env)
    Logger.log(this.getClass, INFO, BaseSLog(s"Running Enviroment :  $environment"))
    Option(environment)
  }

  def getCassandraConnectionAliveTime(streamingInterval: Long): String = {
    (streamingInterval * 1000 + 100).toString
  }


  def doRetry[T](numOfRetries: Int)(body: ⇒ T): T = {
    var retry = 0
    var sleep = 100
    var myError: Throwable = null
    while (retry < numOfRetries) {
      try {
        val a = body
        return a
      }
      catch {
        case e: Throwable =>
          myError = e
          retry = retry + 1
          sleep = sleep * 2
          Thread.sleep(sleep)
      }
    }
    throw new Throwable("RETRY_FAILED", myError)
  }


  def isNullOrEmpty(o: Any): Boolean = o match {
    case m: Map[_, _] => m.isEmpty
    case i: Iterable[Any] => i.isEmpty
    case null | None | EMPTY_STRING => true
    case Some(x) => isNullOrEmpty(x)
    case _ => false
  }

  def md5String(s: String) : String = {
    val md5 = MessageDigest.getInstance("MD5")
    md5.reset()
    md5.update(s.getBytes)
    md5.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
  }

  def queryCassandra(cassandraConnector: CassandraConnector, queryStatement: SimpleStatement): ResultSet = {
    Logger.log(this.getClass, INFO, BaseSLog("[queryCassandra] queryStatement" + queryStatement.getQueryString()))
    cassandraConnector.withSessionDo(session => {
      return session.execute(queryStatement)
    })
  }

  def getAuthentication(authnConfig: Map[String, String]): Authentication = {

    val issueUrl = authnConfig.get(AUTHN_URL).get
    val clientId = authnConfig.get(CLIENT_ID).get
    val clientSecret = authnConfig.get(CLIENT_SECRET).get
    if (!clientId.isEmpty && !clientSecret.isEmpty) {
      val map: Map[String, String] = Map(
        TYPE -> CLIENT_CREDENTIALS,
        "client_id" -> clientId,
        "client_secret" -> clientSecret,
        "issuer_url" -> issueUrl
      )
      val json = Json(DefaultFormats).write(map)
      val encoded = Base64.getEncoder.encodeToString(json.getBytes)
      val data = "data:application/json;base64," + encoded
      val url = new URL(null, data, new DataURLStreamHandler)
      AuthenticationFactoryOAuth2.clientCredentials(new URL(issueUrl), url, AUDIENCE)
    }
    else {
      new AuthenticationDisabled()
    }
  }

  def parseMessage(message: SparkPulsarMessage): Unit = {
    println("Message offset info: " + message.messageId.toString)
    println("Message key: " + message.key)
    println("Message event time: " + message.eventTime)
    println("Message publish time: " + message.publishTime)
    println("Topic name of the message: " + message.topicName)
    println("Custom properties attached to message: " + new ObjectMapper().writeValueAsString(message.properties))
    println("Message body: " + new String(message.data))
  }

  def authnConfigMap():Map[String,String]={
    Map[String, String](
      (AUTHN_URL, Config.getProperty("authn.authnUrl", "https://service.authn-prod.fkcloud.in")),
      (CLIENT_SECRET, Config.getProperty("authn.clientSecret", "Nrdq5PRkHgWh4bJUQzb7KOi+Dmmupl4qLeTgplzW1MNrtT2C")),
      (CLIENT_ID, Config.getProperty("authn.clientId", "flip-dev"))
    )
  }
  def getValue[T](x: T): String = {
    x match {
      case p:(String,String) => p._2
      case q:String => q
      case r: SparkPulsarMessage => new String(r.data)
      case other => throw new IllegalArgumentException("Invalid argument"+other.getClass)
    }
  }
  def main(args: Array[String]): Unit ={
    getAuthentication(authnConfigMap)
  }

}
