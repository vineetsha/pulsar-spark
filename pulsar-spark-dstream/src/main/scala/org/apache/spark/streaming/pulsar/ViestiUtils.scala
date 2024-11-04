package org.apache.spark.streaming.pulsar

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pulsar.client.api.url.PulsarURLStreamHandlerFactory

import java.net.{MalformedURLException, URI, URISyntaxException, URL}
import java.util.Base64
import java.{util => ju}

class ViestiConfig(final val viestiEndpoint: String,final val issuerUrl: String,final val clientId: String,final val clientSecret: String){

  def getClientParams(): ju.Map[String, AnyRef]={
    val clientConf=new ju.HashMap[String, AnyRef]()
    clientConf.put(PulsarContants.ServiceUrlOptionKey, viestiEndpoint)
    clientConf.put(PulsarContants.AuthPluginClassName, "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2")
    clientConf.put(PulsarContants.AuthParams, getAuthParamString())
    clientConf
  }

  def getAuthParamString(): String={
    val mapper=new ObjectMapper()
    val credentialsUrl = buildClientCredentials(clientId, clientSecret, issuerUrl)
    val audience = "authn"
    val authParams=new ju.HashMap[String, String]()
    authParams.put("privateKey", credentialsUrl.toExternalForm)
    authParams.put("issuerUrl",issuerUrl)
    authParams.put("audience", audience)
    mapper.writeValueAsString(authParams)
  }

  @throws[MalformedURLException]
  @throws[URISyntaxException]
  def buildClientCredentials(clientId: String, clientSecret: String, issuerUrl: String): URL = {
    val data = getEncodedData(clientId, clientSecret, issuerUrl)
    val urlStreamHandlerFactory = new PulsarURLStreamHandlerFactory
    val scheme = new URI(data).getScheme
    new URL(null, data, urlStreamHandlerFactory.createURLStreamHandler(scheme))
  }

  private def getEncodedData(clientId: String, clientSecret: String, issuerUrl: String): String = {
    val props = new ju.HashMap[String, String]()
    props.put("type", "client_credentials")
    props.put("client_id", clientId)
    props.put("client_secret", clientSecret)
    props.put("issuer_url", issuerUrl)
    val mapper=new ObjectMapper()
    val json = mapper.writeValueAsString(props)
    val encoded = Base64.getEncoder.encodeToString(json.getBytes)
    "data:application/json;base64," + encoded
  }
}
object ViestiConfig {
  def apply(viestiEndpoint: String, issuerUrl: String, clientId: String, clientSecret: String): ViestiConfig={
    new ViestiConfig(viestiEndpoint, issuerUrl, clientId, clientSecret)
  }





}
