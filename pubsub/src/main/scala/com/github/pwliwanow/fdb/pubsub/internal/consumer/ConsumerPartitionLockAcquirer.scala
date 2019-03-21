package com.github.pwliwanow.fdb.pubsub.internal.consumer

import java.util.UUID

import cats.instances.all._
import cats.syntax.all._
import com.apple.foundationdb.Database
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.internal.locking.{
  ConsumerLock,
  ConsumerLockKey,
  ConsumerLockService,
  ConsumersLockSubspace
}
import com.github.pwliwanow.fdb.pubsub.internal.metadata.TopicMetadataService
import com.github.pwliwanow.foundationdb4s.core.ReadDBIO

import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContextExecutor, Future}

private[pubsub] class ConsumerPartitionLockAcquirer(
    lockService: ConsumerLockService,
    lockSubspace: ConsumersLockSubspace,
    metadataService: TopicMetadataService,
    database: Database) {

  private[pubsub] def releaseLocks(locks: List[ConsumerLock])(
      implicit ec: ExecutionContextExecutor): Future[Unit] = {
    lockService.release(locks, database)
  }

  private[pubsub] def refreshAndAquire(
      subscriptionDetails: SubscriptionDetails,
      existing: List[ConsumerLock],
      consumerId: UUID)(implicit ec: ExecutionContextExecutor): Future[List[ConsumerLock]] = {
    for {
      refreshed <- refreshExistingLocks(existing)
      acquired <- aquireNewLocks(subscriptionDetails, consumerId)
    } yield refreshed ++ acquired
  }

  private def refreshExistingLocks(existing: List[ConsumerLock])(
      implicit ec: ExecutionContextExecutor): Future[List[ConsumerLock]] = {
    existing
      .traverse(lock => lockService.refresh(lock, database))
      .map(_.flatten)
  }

  private def aquireNewLocks(subscriptionDetails: SubscriptionDetails, consumerId: UUID)(
      implicit ec: ExecutionContextExecutor): Future[List[ConsumerLock]] = {
    for {
      (partitions, locksToForceAcquire) <- getAvailablePartitions(subscriptionDetails, consumerId)
      acquireAvailable = acquireAvailableLocksForPartitions(
        subscriptionDetails,
        partitions,
        consumerId)
      forceAcquire = forceAcquireLocks(locksToForceAcquire, consumerId)
      allAcquiredLocks <- acquireAvailable.zipWith(forceAcquire)(_ ++ _)
    } yield allAcquiredLocks
  }

  private def acquireAvailableLocksForPartitions(
      subscriptionDetails: SubscriptionDetails,
      partitions: List[Int],
      consumerId: UUID)(implicit ec: ExecutionContextExecutor): Future[List[ConsumerLock]] = {
    partitions
      .map(ConsumerLockKey(subscriptionDetails.topic, subscriptionDetails.consumerGroup, _))
      .traverse(x => lockService.acquire(x, consumerId, database))
      .map(_.flatten)
  }

  private def forceAcquireLocks(locksToForceAcquire: List[ConsumerLock], consumerId: UUID)(
      implicit ec: ExecutionContextExecutor): Future[List[ConsumerLock]] = {
    locksToForceAcquire
      .traverse(x => lockService.forceAcquire(x, consumerId, database))
      .map(_.flatten)
  }

  // todo acquire those locks gracefully
  private def getAvailablePartitions(subscriptionDetails: SubscriptionDetails, consumerId: UUID)(
      implicit ec: ExecutionContextExecutor): Future[(List[Int], List[ConsumerLock])] = {
    import scala.collection.mutable.{Set => MutableSet}
    val range =
      lockSubspace.range(Tuple.from(subscriptionDetails.topic, subscriptionDetails.consumerGroup))
    val locksDbio: ReadDBIO[(Seq[ConsumerLock], Int)] = for {
      presentLocks <- lockSubspace.getRange(range)
      numberOfPartitions <- metadataService.getNumberOfPartitions(subscriptionDetails.topic)
      // when topic does not exist, acquiring locks will fail,
      // and consumer will just try again in few seconds
    } yield (presentLocks, numberOfPartitions.get)
    locksDbio.transact(database).map {
      case (locks, partitions) =>
        val activeLocks = locks.filter(!lockService.isExpired(_))
        val activeLocksForPartitions = activeLocks.iterator.map(_.key.partition)
        val result = {
          val set = MutableSet.empty[Int]
          set ++= (0 until partitions)
        }
        activeLocksForPartitions.foreach(result -= _)
        val locksToForceAcquire =
          calculateLocksToForceAcquire(activeLocks, result.size, partitions, consumerId)
        (result.toList, locksToForceAcquire)
    }
  }

  private def calculateLocksToForceAcquire(
      activeLocks: Seq[ConsumerLock],
      availableLocks: Int,
      numberOfPartitions: Int,
      consumerId: UUID): List[ConsumerLock] = {
    val consumersWithLocks = activeLocks.groupBy(_.acquiredBy)
    val targetNumberOfConsumers =
      consumersWithLocks.size + consumersWithLocks.get(consumerId).fold(1)(_ => 0)
    val expectedNumberOfLocksToAcquire = numberOfPartitions / targetNumberOfConsumers
    val reminder = numberOfPartitions - (expectedNumberOfLocksToAcquire * targetNumberOfConsumers)
    val existingNumberOfLocksForConsumer =
      consumersWithLocks.get(consumerId).map(_.length).getOrElse(0)
    val numberOfLocksToTake = expectedNumberOfLocksToAcquire - (existingNumberOfLocksForConsumer + availableLocks)

    if (numberOfLocksToTake <= 0) {
      List.empty
    } else {
      val (locksToForceAcquire, _) =
        (consumersWithLocks - consumerId).toList.view
          .sortBy { case (_, locks) => locks.size }(implicitly[Ordering[Int]].reverse)
          .zipWithIndex
          .foldLeft((ListBuffer.empty[ConsumerLock], numberOfLocksToTake)) {
            case ((buff, leftToTake), ((_, locks), i)) =>
              if (leftToTake <= 0) (buff, leftToTake)
              else {
                val numberOfLocksToLeave =
                  if (reminder > i) expectedNumberOfLocksToAcquire + 1
                  else expectedNumberOfLocksToAcquire
                val toTake = Math.min(leftToTake, Math.max(locks.size - numberOfLocksToLeave, 0))
                buff ++= locks.take(toTake)
                (buff, leftToTake - toTake)
              }
          }
      locksToForceAcquire.toList
    }
  }

}
