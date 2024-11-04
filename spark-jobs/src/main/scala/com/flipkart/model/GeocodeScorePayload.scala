package com.flipkart.model

case class GeocodeScorePayload (
   timestamp: java.time.LocalDateTime,
   accountId: String,
   contactId: String,
   addressHash: String,
   address: AddressForLookup,
   source: String,
   location: GeocodeScoreLocation
)

case class GeocodeScoreLocation (lat: Double,
                     lng: Double
                    )
