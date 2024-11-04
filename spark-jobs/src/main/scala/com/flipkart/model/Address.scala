package com.flipkart.model

/**
 * Created by sourav.r on 17/08/16.
 */
import com.fasterxml.jackson.annotation.JsonProperty

class Address(
               var id: String,
               @JsonProperty("type")
               var _type: String,
               var pincode: String,
               var zone: String = null) extends Serializable {

  override def toString = s"Address(id=$id, _type=${_type}, pincode=$pincode, zone = $zone)"

  def removeSpacesInPincode() = {
    if (pincode != null) {
      pincode = pincode.replaceAll("\\s+", "")
    }
  }
}

object Address {
  def apply(id: String, _type: String, pincode: String, zone: String = null) = {
    if (id == null) null else new Address(id, _type, pincode, zone)
  }
}

class AddressForLookup(
                      var addr1: String,
                      var addr2: String,
                      var city: String,
                      var state: String,
                      var pincode: String,
                      var country: String
                      )
