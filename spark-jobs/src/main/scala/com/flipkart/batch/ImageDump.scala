package com.flipkart.batch

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.Calendar

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by sharma.varun on 26/08/15.
 */
object ImageDump {

  def transform(x:CassandraRow): Unit ={

    val source = "/tmp/mount/fsd_signature_dump_27_08_2015_2/"
    val randomGenerator:Random = new Random();
    val tag_id = x.getString("tag_id")
    val png_bytes = x.getBytes("signature")
    val time = x.getDate("time")
    val calendar = Calendar.getInstance();
    calendar.setTime(time)
    val date = calendar.get(Calendar.DATE) + "_"+calendar.get(Calendar.MONTH) + "_" + calendar.get(Calendar.YEAR)
    val folder = source + date + "/" + randomGenerator.nextInt(500)
    val f = new File(folder + "/" + tag_id + ".png")
    f.getParentFile().mkdirs()
    val wChannel:FileChannel = new FileOutputStream(f, false).getChannel()
    wChannel.write(png_bytes)
    wChannel.close()
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("ImageDump"))
    val rdd = sc.cassandraTable("compass", "signature").select("tag_id","src","type","time","signature")
    val rdd2=rdd.map(x=>x.getString("tag_id"))distinct()
    rdd.foreach(transform)
    //or
    rdd.map{x=>
      val outputs = new MultiWriter("/tmp/mount/signature_dump_27_08_2015_2/")
      val calendar = Calendar.getInstance();
      calendar.setTime(x.getDate("time"))
      val date = calendar.get(Calendar.DATE) + "_" + calendar.get(Calendar.MONTH) + "_" + calendar.get(Calendar.YEAR)
      outputs.write(date, x.getString("tag_id"), x.getBytes("signature"))
      Nil
    }.foreach(x=>x)
  }

}
class MultiWriter(source: String) {
  def write(date: String, tag_id:String, value: ByteBuffer) = {
    val randomGenerator: Random = new Random();
    val folder = source + date + "/" + randomGenerator.nextInt(500)
    val f = new java.io.File(folder + "/" + tag_id + ".png")
    f.getParentFile.mkdirs
    val channel = new FileOutputStream(f, false).getChannel()
    channel.write(value)
    channel.close()
  }
}
