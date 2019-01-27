package com.github.pwliwanow.fdb.pubsub.internal.locking

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.internal.common.SerDe
import com.github.pwliwanow.foundationdb4s.core.SubspaceWithVersionstampedValues

private[pubsub] final class ConsumersLockSubspace(val subspace: Subspace)
    extends SubspaceWithVersionstampedValues[ConsumerLock, ConsumerLockKey] {

  override def toKey(entity: ConsumerLock): ConsumerLockKey = entity.key

  override def extractVersionstamp(entity: ConsumerLock) = entity.acquiredWith

  override def toTupledValue(entity: ConsumerLock): Tuple =
    Tuple.from(
      entity.acquiredWith,
      SerDe.encodeInstant(entity.refreshedAt): java.lang.Long,
      entity.acquiredBy)

  override def toEntity(key: ConsumerLockKey, tupledValue: Tuple) =
    ConsumerLock(
      key = key,
      acquiredWith = tupledValue.getVersionstamp(0),
      refreshedAt = SerDe.decodeInstant(tupledValue.getLong(1)),
      acquiredBy = tupledValue.getUUID(2))

  override def toTupledKey(key: ConsumerLockKey): Tuple =
    Tuple.from(key.topic, key.consumerGroup, SerDe.encodeInt(key.partition))

  override def toKey(tupledKey: Tuple): ConsumerLockKey =
    ConsumerLockKey(
      topic = tupledKey.getString(0),
      consumerGroup = tupledKey.getString(1),
      partition = SerDe.decodeInt(tupledKey.getBytes(2)))

}
