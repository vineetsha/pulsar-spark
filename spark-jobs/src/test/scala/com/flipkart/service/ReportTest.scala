package com.flipkart.service

import java.io.{FileWriter, BufferedWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.driver.core._
import com.datastax.spark.connector.UDTValue
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.flipkart.batch.{GoodData, AccuracyAnalysis}
import com.flipkart.config.Config
import com.flipkart.utils._
import com.google.common.reflect.TypeToken
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import com.datastax.spark.connector._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scalax.chart.api._

/**
 * Created by sourav.r on 05/04/16.
 */
class ReportTest extends FunSuite with SharedSparkContext with BeforeAndAfter{
  var zkPropertiesGeoCode: Map[String,String] = Map[String,String]()
  var configMap:Map[String, String]= Map[String,String]()

  after{
    //CassandraClient.getClient(configMap.apply("cassandraHost")).getSession(). execute(new SimpleStatement("TRUNCATE compass.geocode_validation"))
  }
  before{
    zkPropertiesGeoCode += "group.id" -> Config.getProperty("Connections.KAFKA_CONSUMER.geoCode", "geoCode")
    zkPropertiesGeoCode += "zookeeper.connect" -> Config.getProperty("Connections.KAFKA_CONSUMER.zookeeper", "ekl-lmp-1.stage.ch.flipkart.com:2181")

    configMap += "geoCoderEndPoint" -> Config.getProperty("geoCoder.Geocode.Url", "http://ekl-routes-1.nm.flipkart.com:8888")
    configMap += "geoCoderEndPoint2" -> Config.getProperty("geoCoder2.Geocode.Url", "http://ekl-lmp-4.stage.nm.flipkart.com:5555")
    configMap += "fsdExternalEndPoint" -> Config.getProperty("fsd.external.Url", "http://flo-fkl-app2.stage.ch.flipkart.com:27012/fsd-external-apis")
    configMap += "facilityEndPoint" -> Config.getProperty("facility.Url", "http://flo-fkl-app5.stage.ch.flipkart.com:27747")
    configMap += "userGeoCodingMsgQUrl" -> Config.getProperty("UserGeoCoding.MsgQ.url","http://10.65.38.218/queues/ekl-compass_production/messages")
    configMap += "userGeoCodingServiceUrl" -> Config.getProperty("UserGeoCoding.Service.url","http://ekl-routes-5.nm.flipkart.com:5001/user_geocode/invoke")
    configMap += "mmiGeoCodingMsgQUrl" -> Config.getProperty("MmiGeoCoding.MsgQ.Url","")
    configMap += "mmiGeoCodingServiceUrl"->(Config.getProperty("MmiGeoCoding.Service.Url",""))
    configMap += "cassandraHost" -> "10.84.56.173"
    configMap += "fsdExternalEndPoint" -> "http://erp-logis-api.vip.nm.flipkart.com/fsd-external-apis"
    configMap += "facilityEndPoint" -> "http://ekl-facilities.nm.flipkart.com"
  }

  override def beforeAll(): Unit = {
    conf.set("spark.cassandra.connection.host","10.84.56.173")
    super.beforeAll()
  }

  def getDataFromGeotag(device_id :String, date:String): ResultSet ={
    return CassandraClient
      .getClient(configMap.apply("cassandraHost"))
      .getSession()
      .execute(new SimpleStatement("SELECT * FROM compass.geotag WHERE device_id = ? and date = ?",device_id,date))
  }

  def data_array(rdd: CassandraTableScanRDD[CassandraRow]): RDD[(String, String,Double, Double, Double, String)] = {
    val data = rdd.filter(x => x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "accuracy_level") != Nil)
      .map(x => {
      (
        x.getString("device_id"),
        x.getString("date"),
        x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "accuracy_level").head.getString("value").toDouble,
        x.getDouble("lat"),
        x.getDouble("lng"),
        x.getString("src")
        )
    })
    data
  }

  test("test accuracy report"){
    val geoTagRdd1 = sc.cassandraTable("compass", "geotag").select("device_id", "date","time", "attributes", "lat", "lng", "src").where("device_id = ? and date = ?","911380455089718","2016-04-04")
    val geoTagRdd2 = sc.cassandraTable("compass", "geotag").select("device_id", "date","time", "attributes", "lat", "lng", "src").where("device_id = ? and date = ?","353324063944463","2016-04-04")

    val geoTagAccuracyData = data_array(geoTagRdd1)
      .filter(x=>(x._2 == "2016-04-04"))
      .map(x=>(x._2,x._3))
      .groupByKey()
      .map(x=>AccuracyAnalysis.buildPercentileTuple("geotag",x))
      .collect()

    val geotagAccuracySplit = data_array(geoTagRdd1).filter(x=>(x._2 == "2016-04-04"))
      .map(x=>(x._2,x._3))
      .groupByKey()
      .map(x=>AccuracyAnalysis.buildCountSplitTuple("geotag",x))
      .collect()

    val out = new BufferedWriter(new FileWriter("accuracy.csv"))
    val writer = new CSVWriter(out)
    writer.writeNext(("type","date","Samples","Accuracy(50 percentile)","Accuracy(90 percentile)","Accuracy(mean)","Accuracy(Std. Dev.)").productIterator.toArray.map(x => x.toString))
    for (elem <- geoTagAccuracyData) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }

    writer.writeNext(("type","date","Samples","(<0)","(>0-50)","(>50-100)","(>100-500)","(>500)").productIterator.toArray.map(x => x.toString))
    var pieSample = geotagAccuracySplit.map(x =>
      {List((("<=0"),x._4),(("0 to 50"),x._5),(("50 to 100"),x._6),(("100 to 500"),x._7),((">= 500"),x._8))}
    ).flatMap(x=>x)

    var v = ArrayBuffer[(String,Int)]()
    for( elem <- pieSample){
      v += elem
    }
    PieChart(v).saveAsPNG("/tmp/data-accracy.png")

    for (elem <- geotagAccuracySplit) {
      writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
    }
    writer.close()
  }

  test("test dump Good Data"){
    val geoTagRdd1 = sc.cassandraTable("compass", "geotag").select("device_id", "date", "src", "type", "time", "addr", "addr_full", "alt", "attributes", "lat", "lng", "tag_id").where("device_id = ? and date = ?","911380455089718","2016-04-04")

    GoodData.dump_geotag_good(geoTagRdd1)
  }
}
