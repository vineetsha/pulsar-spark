package com.flipkart.core

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Created by shivang.b on 11/26/15.
 */
case class GeoTagBigfootPayload(eventId: String,
                         data: GeoTagData,
                          eventTime: String,
                          schemaVersion: String)

case class GeoTagData(tracking_id:String,
                              date:String,
                              tag_id:String,
                              time:String,
                              latitude:Double,
                              longitude:Double,
                              altitude:Double,
                              source:String,
                              @JsonProperty("type")
                              _type:String,
                              address_hash:String,
                              address_full:String,
                              address:Map[String, String],
                              attributes:List[Map[String, String]])