package com.flipkart.model

case class CacheRefreshPayload (
   accountId: String,
   contactId: String,
   addr1: String,
   addr2: String,
   city: String,
   state: String,
   pincode: String,
   country: String,
   updated_lat: Double,
   updated_lng: Double,
   cluster_score: Double
)
