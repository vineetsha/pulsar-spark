package com.flipkart.utils

import com.datastax.spark.connector.UDTValue
import com.flipkart.model.AddressForLookup

import scala.collection.mutable.ListBuffer

/**
 * Created by shivang.b on 12/28/15.
 */
object KeyValuePairGenerator {

  def fromMapToList(attributes:Map[String,String]):
    List[Map[String, String]] ={
      var list = new ListBuffer[Map[String, String]]
      for ((key, value) <- attributes) {
        var attributeMap = Map("key" -> key, "value" -> value)
        list += attributeMap
      }
      list.toList
  }

  def fromMapToUDTList(attributes:Map[String,String]):
  List[UDTValue] = {
    var list = new ListBuffer[UDTValue]
    for ((key, value) <- attributes) {
      var udtValue = UDTValue.fromMap(Map("key" -> key, "value" -> value))
      list += udtValue
    }
    list.toList
  }

  def UDTListToAddressForLookup(udtList: List[UDTValue]):
  AddressForLookup = {
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
    new AddressForLookup(addr1, addr2, city, state, pincode, country)
  }
}
