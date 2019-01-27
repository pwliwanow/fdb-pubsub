package com.github.pwliwanow.fdb.pubsub.internal.common

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.internal.metadata.TopicMetadata
import com.github.pwliwanow.foundationdb4s.core.SubspaceWithVersionstampedValues

private[pubsub] class TopicMetadataSubspace(val subspace: Subspace)
    extends SubspaceWithVersionstampedValues[TopicMetadata, String] {

  override def extractVersionstamp(entity: TopicMetadata) = entity.createdIn

  override def toKey(entity: TopicMetadata) = entity.topic

  override def toKey(tupledKey: Tuple) = tupledKey.getString(0)

  override def toTupledValue(entity: TopicMetadata): Tuple =
    Tuple.from(
      SerDe.encodeInt(entity.numberOfPartitions),
      SerDe.encodeInstant(entity.createdAt): java.lang.Long,
      entity.createdIn)

  override def toEntity(key: String, tupledValue: Tuple): TopicMetadata =
    TopicMetadata(
      topic = key,
      numberOfPartitions = SerDe.decodeInt(tupledValue.getBytes(0)),
      createdAt = SerDe.decodeInstant(tupledValue.getLong(1)),
      createdIn = tupledValue.getVersionstamp(2)
    )

  override def toTupledKey(key: String): Tuple =
    Tuple.from(key)

}
