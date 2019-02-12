package com.github.pwliwanow.fdb.pubsub.internal.metadata

import com.apple.foundationdb.tuple.Versionstamp
import com.github.pwliwanow.fdb.pubsub.internal.common.TopicRecord
import com.github.pwliwanow.fdb.pubsub.internal.locking.ConsumerLock
import com.github.pwliwanow.fdb.pubsub.internal.metadata.UpdateLockResult.UpdateLockResult
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO}

import scala.math.Ordered._

private[pubsub] class ConsumerGroupMetadataService(subspace: ConsumerGroupMetadataSubspace) {

  def updateLastApplied(
      consumerGroup: String,
      topicRecord: TopicRecord,
      lock: ConsumerLock): DBIO[UpdateLockResult] = {
    val key = ConsumerGroupMetadataKey(
      topic = topicRecord.topicKey.topic,
      consumerGroup = consumerGroup,
      partitionNo = topicRecord.topicKey.partition)
    def doUpdate(): DBIO[UpdateLockResult] =
      subspace
        .set(
          ConsumerGroupMetadata(
            key = key,
            offset = topicRecord.topicKey.versionstamp,
            updatedWithLock = lock.acquiredWith))
        .map(_ => UpdateLockResult.Success)
    for {
      maybeExisting <- subspace.get(key).toDBIO
      result <- maybeExisting.fold(doUpdate()) { x =>
        if (x.updatedWithLock > lock.acquiredWith) {
          DBIO.pure(UpdateLockResult.OutdatedLock)
        } else if (x.offset == topicRecord.topicKey.versionstamp) {
          DBIO.pure(UpdateLockResult.OffsetAlreadyCommitted)
        } else if (x.offset >= topicRecord.topicKey.versionstamp) {
          DBIO.pure(UpdateLockResult.OffsetTooOld)
        } else {
          doUpdate()
        }
      }
    } yield result
  }

  def getOffset(
      topic: String,
      consumerGroup: String,
      partitionNo: Int): ReadDBIO[Option[Versionstamp]] = {
    val key = ConsumerGroupMetadataKey(
      topic = topic,
      consumerGroup = consumerGroup,
      partitionNo = partitionNo)
    subspace.get(key).map(_.map(_.offset))
  }

}

private[pubsub] object UpdateLockResult extends Enumeration {
  type UpdateLockResult = Value
  val OutdatedLock, OffsetAlreadyCommitted, OffsetTooOld, Success = Value
}
