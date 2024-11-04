package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.api.{Authentication, AuthenticationDataProvider, AuthenticationFactory, PulsarClient}
import org.apache.pulsar.client.impl.auth.AuthenticationToken
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import java.{util => ju}

object PulsarProducer{
  def produceMessages(producerConfigs: ju.Map[String, AnyRef],
                      authentication: Authentication,
                      serviceUrl: String
                      ): Unit={
    val pulsarClient= PulsarClient.builder()
      .serviceUrl(serviceUrl)
      .authentication(authentication)
      .build();
  }

  def main(args: Array[String]): Unit = {
//    AuthenticationFactoryOAuth2.clientCredentials()
    var auth: Authentication = AuthenticationFactory.token("")

  }
}
