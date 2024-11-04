package com.flipkart.core

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.Calendar

import com.fasterxml.jackson.annotation.JsonProperty
import com.flipkart.model.SignatureCassandraModel
import sun.misc.BASE64Decoder

/**
  * Created by sharma.varun on 17/10/15.
  */

class SignaturePayload(tag_id:String,
                     source:String,
                     @JsonProperty("type")
                     _type:String,
                     time:String ,
                     signature:String) {

  private def convertToBlob(stream: String): ByteBuffer = {
    ByteBuffer.wrap(new BASE64Decoder().decodeBuffer(stream))
  }

  def convertToCassandraModel(): SignatureCassandraModel = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val cur_date = dateFormat.format(Calendar.getInstance().getTime)
    new SignatureCassandraModel(this.tag_id,
      this.source,
      this._type,
      if (this.time != null) this.time else cur_date,
      convertToBlob(this.signature))
  }
}
