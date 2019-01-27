package com.github.pwliwanow.fdb.pubsub.internal.consumer

import akka.stream.scaladsl.Source
import com.apple.foundationdb.{Database, KeySelector}
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.internal.common.{SerDe, TopicRecord, TopicSubspace}
import com.github.pwliwanow.fdb.pubsub.internal.locking.ConsumerLock
import com.github.pwliwanow.fdb.pubsub.internal.metadata.ConsumerGroupMetadataService
import com.github.pwliwanow.fdb.pubsub.internal.metadata.UpdateLockResult.UpdateLockResult
import com.github.pwliwanow.foundationdb4s.core.DBIO
import com.github.pwliwanow.foundationdb4s.streams.InfinitePollingSubspaceSource

import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor

private[pubsub] final case class SubscriptionDetails(
    topic: String,
    consumerGroup: String,
    pollingInterval: FiniteDuration)

private[pubsub] class ConsumerService(
    consumerGroupMetadataService: ConsumerGroupMetadataService,
    topicSubspace: TopicSubspace,
    database: Database) {

  def createSource(lock: ConsumerLock, details: SubscriptionDetails)(
      implicit ec: ExecutionContextExecutor): Source[TopicRecord, _] = {
    val partitionNo = lock.key.partition
    val futureSource =
      consumerGroupMetadataService
        .getOffset(details.topic, details.consumerGroup, partitionNo)
        .transact(database)
        .map { maybeOffset =>
          val baseTuple = Tuple.from(details.topic, SerDe.encodeInt(partitionNo))
          val wholeRange = topicSubspace.range(baseTuple)
          val start = maybeOffset.fold(baseTuple)(baseTuple.add)
          val startRange = topicSubspace.range(start)
          InfinitePollingSubspaceSource.from(
            topicSubspace,
            database,
            details.pollingInterval,
            KeySelector.firstGreaterOrEqual(startRange.begin),
            KeySelector.firstGreaterOrEqual(wholeRange.end))
        }
    Source.fromFutureSource(futureSource)
  }

  def createCommitAction(
      record: TopicRecord,
      lock: ConsumerLock,
      details: SubscriptionDetails): DBIO[UpdateLockResult] = {
    consumerGroupMetadataService.updateLastApplied(details.consumerGroup, record, lock)
  }

}
