package com.github.pwliwanow.fdb.pubsub.internal.metadata

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.internal.common.SerDe
import com.github.pwliwanow.foundationdb4s.core.TypedSubspace

private[pubsub] class ConsumerGroupMetadataSubspace(val subspace: Subspace)
    extends TypedSubspace[ConsumerGroupMetadata, ConsumerGroupMetadataKey] {

  override def toKey(entity: ConsumerGroupMetadata): ConsumerGroupMetadataKey = entity.key

  override def toRawValue(entity: ConsumerGroupMetadata): Array[Byte] = {
    Tuple.from(entity.offset, entity.updatedWithLock).pack
  }

  override def toTupledKey(key: ConsumerGroupMetadataKey): Tuple = {
    Tuple.from(key.topic, key.consumerGroup, SerDe.encodeInt(key.partitionNo))
  }

  override def toKey(tupledKey: Tuple): ConsumerGroupMetadataKey = {
    ConsumerGroupMetadataKey(
      topic = tupledKey.getString(0),
      consumerGroup = tupledKey.getString(1),
      partitionNo = SerDe.decodeInt(tupledKey.getBytes(2)))
  }

  override def toEntity(
      key: ConsumerGroupMetadataKey,
      value: Array[Byte]): ConsumerGroupMetadata = {
    val tupledValue = Tuple.fromBytes(value)
    ConsumerGroupMetadata(
      key = key,
      offset = tupledValue.getVersionstamp(0),
      updatedWithLock = tupledValue.getVersionstamp(1))
  }

}
