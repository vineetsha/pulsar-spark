package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.impl.conf.{ClientConfigurationData, ConsumerConfigurationData}
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnore

import java.lang.reflect.Modifier
import java.{util => ju}
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.reflect.{ClassTag, classTag}

private[pulsar] object PulsarConfigurationUtils {

  private def nonIgnoredFields[T: ClassTag] = {
    classTag[T].runtimeClass.getDeclaredFields
      .filter(f => !Modifier.isStatic(f.getModifiers))
      .filter(f => f.getDeclaredAnnotation(classOf[JsonIgnore]) == null)
      .map(_.getName)
  }

  private def insensitive2Sensitive[T: ClassTag]: ju.Map[String, String] = {
    nonIgnoredFields[T].map(s => s -> s).toMap[String,String].asJava
  }

  val clientConfKeys: ju.Map[String, String] = insensitive2Sensitive[ClientConfigurationData]
  val readerConfKeys: ju.Map[String, String] = insensitive2Sensitive[ConsumerConfigurationData[_]]
}
