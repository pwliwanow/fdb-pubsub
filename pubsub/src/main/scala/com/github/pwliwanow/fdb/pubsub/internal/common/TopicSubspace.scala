package com.github.pwliwanow.fdb.pubsub.internal.common

import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.github.pwliwanow.foundationdb4s.core.SubspaceWithVersionstampedKeys

private[pubsub] final class TopicSubspace(val subspace: Subspace)
    extends SubspaceWithVersionstampedKeys[TopicRecord, TopicKey] {

  override def toKey(entity: TopicRecord): TopicKey = entity.topicKey

  override def toRawValue(entity: TopicRecord): Array[Byte] =
    Tuple
      .from(
        entity.userData.getKey,
        entity.userData.getValue,
        SerDe.encodeInstant(entity.createdAt): java.lang.Long)
      .pack

  override def toTupledKey(key: TopicKey): Tuple =
    Tuple.from(key.topic, SerDe.encodeInt(key.partition), key.versionstamp)

  override def toKey(tupledKey: Tuple): TopicKey =
    TopicKey(
      topic = tupledKey.getString(0),
      partition = SerDe.decodeInt(tupledKey.getBytes(1)),
      versionstamp = tupledKey.getVersionstamp(2))

  override def toEntity(key: TopicKey, value: Array[Byte]): TopicRecord = {
    val tupledValue = Tuple.fromBytes(value)
    val userKey = tupledValue.getBytes(0)
    val userValue = tupledValue.getBytes(1)
    val createdAt = SerDe.decodeInstant(tupledValue.getLong(2))
    TopicRecord(key, userData = new KeyValue(userKey, userValue), createdAt)
  }

  override def extractVersionstamp(key: TopicKey): Versionstamp = key.versionstamp

}
