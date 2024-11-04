package com.flipkart.core

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.Calendar

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.flipkart.model.SignatureCassandraModel
import sun.misc.BASE64Decoder

/**
 * {
	"externalTrackingId": "FMPR4289177311",
	"shipmentAttributes": [{
		"name": "dangerous",
		"value": "false"
	}, {
		"name": "source_type",
		"value": "fbf"
	}, {
		"name": "try_and_buy",
		"value": "false"
	}, {
		"name": "priority_value",
		"value": "NON-PRIORITY"
	}, {
		"name": "tier_type",
		"value": "REGULAR"
	}, {
		"name": "seller_return",
		"value": "false"
	}, {
		"name": "print_customer_form",
		"value": "false"
	}],
	"deliveryCustomerCity": "Pune",
	"deliveryCustomerCountry": "",
	"deliveryCustomerPincode": "411003",
	"deliveryCustomerAddress1": "Tital Areation Co. (P) Ltd. SC 1/14 Kohinoor Estate, Behind Kamal Nayan Bajaj Udyan, Mumbai-Pune-Road, Pune 411046",
	"deliveryCustomerAddress2": "",
	"deliveryCustomerName": "Amruta Gore",
	"deliveryCustomerState": "Maharashtra",
	"deliveryCustomerPhone": "9850378167",
	"deliveryCustomerEmail": "vikyspt@gmail.com",
  "address_id": "CNTCT8083DEA663F84E5EB981F979D"

}
  */
@JsonIgnoreProperties(ignoreUnknown = true)
class GeoCodeValidationPayload(var externalTrackingId:String,
                               var shipmentAttributes:List[Map[String,String]],
                               var deliveryCustomerCity:String,
                               var deliveryCustomerPincode:String,
                               var deliveryCustomerAddress1:String,
                               var deliveryCustomerAddress2:String,
                               var deliveryCustomerName:String,
                               var deliveryCustomerPhone:String ,
                               var deliveryCustomerEmail:String,
                               var deliveryCustomerState:String,
                               var deliveryCustomerCountry:String,
                               var address_id:String
                     ) {
}
