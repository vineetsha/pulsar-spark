package org.apache.spark.streaming.pulsar

import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import org.apache.spark.Partition
import org.apache.spark.streaming.pulsar.PulsarPartition.PulsarPartitionTuple

import java.io.{Externalizable, ObjectInput, ObjectOutput}

private[pulsar] case class SparkPulsarPartition(index: Int, pulsarPartition: PulsarPartition)
  extends Partition


private[pulsar] class SchemaInfoSerializable(var si: SchemaInfo) extends Externalizable {

  def this() = this(null) // For deserialization only

  override def writeExternal(out: ObjectOutput): Unit = {
    val schema = si.getSchema
    if (schema.isEmpty) {
      out.writeInt(0)
    } else {
      out.writeInt(schema.length)
      out.write(schema)
    }

    out.writeUTF(si.getName)
    out.writeObject(si.getProperties)
    out.writeInt(si.getType.getValue)
  }

  override def readExternal(in: ObjectInput): Unit = {
    val len = in.readInt()
    var schema = new Array[Byte](0)
    if (len > 0) {
      schema = new Array[Byte](len)
      in.readFully(schema)
    }
    val schemaName = in.readUTF()
    val properties = in.readObject().asInstanceOf[java.util.Map[String, String]]
    val schemaType = SchemaType.valueOf(in.readInt())

    si = SchemaInfoImpl
      .builder()
      .name(schemaName)
      .schema(schema)
      .`type`(schemaType)
      .properties(properties)
      .build()
  }
}

final class PulsarPartition(val topic: String,
                            val pulsarOffsetRange: PulsarOffsetRange,
                            val siSerializable: SchemaInfoSerializable) extends Serializable {
  override def equals(obj: Any): Boolean = obj match {
    case that: PulsarPartition =>
      this.topic == that.topic &&
        this.pulsarOffsetRange == that.pulsarOffsetRange
    case _ => false
  }

  override def hashCode(): Int = {
    toTuple.hashCode()
  }

  override def toString(): String = {
    s"OffsetRange(topic: '$topic', StartMessageId: ${pulsarOffsetRange.startMessageId}, EndMessageId:${pulsarOffsetRange.endMessageId} BatchPolicy: ${pulsarOffsetRange.batchSize})"
  }

  def toTuple: PulsarPartitionTuple = (topic, pulsarOffsetRange, siSerializable)

}

object PulsarPartition {
  def apply(topic: String, pulsarOffsetRange: PulsarOffsetRange, siSerializable: SchemaInfoSerializable): PulsarPartition = {
    new PulsarPartition(topic, pulsarOffsetRange, siSerializable)
  }

  def apply(topic: String, pulsarOffsetRange: PulsarOffsetRange, si: SchemaInfo): PulsarPartition = {
    new PulsarPartition(topic, pulsarOffsetRange, new SchemaInfoSerializable(si))
  }

  type PulsarPartitionTuple = (String, PulsarOffsetRange, SchemaInfoSerializable)

  def apply(t: PulsarPartitionTuple) =
    new PulsarPartition(t._1, t._2, t._3)
}

