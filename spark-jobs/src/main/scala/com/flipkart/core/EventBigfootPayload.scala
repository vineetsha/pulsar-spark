package com.flipkart.core

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Created by shivang.b on 12/21/15.
 */
case class EventBigfootPayload(eventId: String,
                                data: EventData,
                                eventTime: String,
                                schemaVersion: String)

case class EventData(tracking_id:String,
                      date:String,
                      time:String,
                      latitude:Double,
                      longitude:Double,
                      altitude:Double,
                      source:String,
                      @JsonProperty("type")
                      _type:String,
                      attributes:List[Map[String, String]])
