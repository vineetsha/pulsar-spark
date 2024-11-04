package com.flipkart.model

/**
 * Created by sourav.r on 17/08/16.
 */
import com.fasterxml.jackson.annotation._

class AddressEntity(
                     var id: String,
                     @JsonProperty("type")
                     var _type: String,
                     var pincode: String
                     ) extends Serializable {
}

@JsonIgnoreProperties(ignoreUnknown = true)
class ShipmentEntity(
                      var status: String,
                      var shipment_type: String,
                      var vendor_tracking_id: String,
                      var destination_address: AddressEntity
                      ) extends Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonRootName("entity")
class ShipmentPayload(
                       var ingestedAt: Long,
                       var entityId: String,
                       var updatedAt: Long,
                       var data: ShipmentEntity
                       ) extends Serializable {
}

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonRootName("entity")
class ShipmentBigfootEntry(var entity: ShipmentPayload ) extends Serializable {}