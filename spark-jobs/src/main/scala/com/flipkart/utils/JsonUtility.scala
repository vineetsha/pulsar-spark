package com.flipkart.utils

import java.lang.reflect.{ParameterizedType, Type}
import java.util.{Calendar, Date, TimeZone}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.liftweb.json._
import org.apache.spark.rdd.RDD

/**
 * Created by sharma.varun on 13/10/15.
 */


object defaultDateFormats extends Formats {
  val SYSTEM_TIMEZONE = TimeZone.getTimeZone(Calendar.getInstance().getTimeZone.getDisplayName)
  import java.text.{ParseException, SimpleDateFormat}

  val dateFormat = new DateFormat {
    def parse(s: String) = try {
      Some(formatter.parse(s))
    } catch {
      case e: ParseException => None
    }

    def format(d: Date) = formatter.format(d)

    private def formatter = {
      val f = simpleDateFormatter
      f.setTimeZone(defaultDateFormats.SYSTEM_TIMEZONE)
      f
    }
  }

  protected def simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")

  def lossLessDate = new DefaultFormats {
    override def dateFormatter = simpleDateFormatter
  }

  /** Default formats with given <code>TypeHint</code>s.
    */
  def withHints(hints: TypeHints) = new DefaultFormats {
    override val typeHints = hints
  }
}

object JsonUtility {

  implicit val formats = defaultDateFormats

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(SerializationFeature.INDENT_OUTPUT, true)


  def serialize(value: Any): String = {
    import java.io.StringWriter
    val writer = new StringWriter()
    mapper.writeValue(writer, value)
    writer.toString
  }

  def deserialize[T: Manifest](value: String) : T =
      mapper.readValue(value, typeReference[T])

  def deserialize[T: Manifest](values: RDD[String]) : RDD[T] =
    values.map(x => deserialize(x))

  def pretty(json:String ) :String = {
    val tree = mapper.readTree(json)
    mapper.writeValueAsString(tree)
  }

    private [this] def typeReference[T: Manifest] = new TypeReference[T] {
      override def getType = typeFromManifest(manifest[T])
    }

    private [this] def typeFromManifest(m: Manifest[_]): Type = {
      if (m.typeArguments.isEmpty) { m.runtimeClass }
      else new ParameterizedType {
        def getRawType = m.runtimeClass
        def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
        def getOwnerType = null
      }
    }



}
