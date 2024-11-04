package com.flipkart.batch

import java.io.{BufferedWriter, FileWriter}

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
object BarCodePinData {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("AccuracyReport"))
        val rdd = sc.cassandraTable("compass", "geotag").select("device_id", "date", "src", "type", "time", "addr", "addr_full", "alt", "attributes", "lat", "lng", "tag_id")
        val data = rdd.filter(x => x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "verified_by" && (x.getString("value") == "barcode" || x.getString("value") == "pin")) != Nil)

        val out = new BufferedWriter(new FileWriter("gooddata6.csv"));
        val writer = new CSVWriter(out);

        //rdd.filter(x => x.getList[UDTValue]("attributes").filter(x=> x.getString("key")=="verified_by"&&x.getString("value").isEmpty())!=Nil).count
        writer.writeNext(Array("device_id", "tag_id", "lat", "lng", "alt", "date", "src", "type", "time", "addr_full", "addr1", "addr2", "city", "state", "pincode", "country", "accuracy_level"))
        val mappeddata = data.map(x => (x.getString("device_id"), x.getString("tag_id"), x.getString("lat"), x.getString("lng"), x.getString("alt"), x.getString("date"), x.getString("src"), x.getString("type"), x.getString("time"), x.getString("addr_full"), x.getList[UDTValue]("addr").filter(x => x.getString("key") == "addr1")(0).getString("value"), x.getList[UDTValue]("addr").filter(x => x.getString("key") == "addr2")(0).getString("value"), x.getList[UDTValue]("addr").filter(x => x.getString("key") == "city")(0).getString("value"), x.getList[UDTValue]("addr").filter(x => x.getString("key") == "state")(0).getString("value"), x.getList[UDTValue]("addr").filter(x => x.getString("key") == "pincode")(0).getString("value"), x.getList[UDTValue]("addr").filter(x => x.getString("key") == "country")(0).getString("value"), x.getList[UDTValue]("attributes").filter(x => x.getString("key") == "accuracy_level")(0).getString("value"))).collect()
        for (elem <- mappeddata) {
            writer.writeNext(elem.productIterator.toArray.map(x => x.toString))
        }

    }
}