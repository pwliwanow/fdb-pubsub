package com.github.pwliwanow.fdb.pubsub.internal.consumer

import java.util.UUID

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream._
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.apple.foundationdb.KeyValue
import com.github.pwliwanow.fdb.pubsub
import com.github.pwliwanow.fdb.pubsub.ConsumerRecord
import com.github.pwliwanow.fdb.pubsub.internal.locking.ConsumerLock
import com.github.pwliwanow.fdb.pubsub.internal.metadata.UpdateLockResult
import com.github.pwliwanow.foundationdb4s.core.DBIO

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

private[pubsub] class PartitionSources(
    consumerService: ConsumerService,
    lockAcquirer: ConsumerPartitionLockAcquirer,
    subscriptionDetails: SubscriptionDetails,
    acquireLocksInitialDelay: FiniteDuration,
    acquireLocksInterval: FiniteDuration)
    extends GraphStage[SourceShape[Source[ConsumerRecord[KeyValue], NotUsed]]] {

  private val out: Outlet[Source[ConsumerRecord[KeyValue], NotUsed]] = Outlet(
    "PartitionSourcesSource.out")

  override def shape: SourceShape[Source[ConsumerRecord[KeyValue], NotUsed]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    new GraphStageLogic(shape) with OutHandler { stageLogic =>
      private implicit def ec: ExecutionContextExecutor = materializer.executionContext

      private val id = UUID.randomUUID()
      private var acquiredLocks = List.empty[ConsumerLock]

      class StreamData(
          val killSwitch: UniqueKillSwitch,
          val source: Source[ConsumerRecord[KeyValue], NotUsed],
          val lock: ConsumerLock,
          var wasPushed: Boolean)

      private var existingStreams = List.empty[StreamData]
      private var scheduledLockAcquiring: Cancellable = _

      setHandler(out, this)

      override def preStart(): Unit = {
        scheduledLockAcquiring = materializer.scheduleAtFixedRate(
          initialDelay = acquireLocksInitialDelay,
          interval = acquireLocksInterval,
          task = () => {
            lockAcquirer
              .refreshAndAquire(subscriptionDetails, acquiredLocks, id)
              .foreach(acquiredLocksCallback.invoke)
          }
        )
      }

      override def postStop(): Unit = {
        val locks = existingStreams.map(_.lock)
        existingStreams.foreach(_.killSwitch.shutdown())
        existingStreams = List.empty
        scheduledLockAcquiring.cancel()
        Await.result(lockAcquirer.releaseLocks(locks), 3.seconds)
      }

      override def onPull(): Unit = {
        if (isAvailable(out)) {
          existingStreams.iterator
            .find(!_.wasPushed)
            .foreach { streamData =>
              push(out, streamData.source)
              streamData.wasPushed = true
            }
        }
      }

      private def acquiredLocksCallback = getAsyncCallback[List[ConsumerLock]] { locks =>
        acquiredLocks = locks
        val currentLocks = existingLocksForPartitions()
        val (present, toAdd) =
          locks.partition(l => currentLocks.get(l.key.partition).isDefined)
        val (toLeave, toClose) =
          existingStreams.partition(streamData => present.exists(_.key == streamData.lock.key))
        toClose.foreach(_.killSwitch.shutdown())

        val newStreams =
          toAdd
            .map { l =>
              val (killSwitch, source) =
                consumerService
                  .createSource(l, subscriptionDetails)
                  .map { record =>
                    val commitAction: DBIO[Boolean] =
                      consumerService
                        .createCommitAction(record, l, subscriptionDetails)
                        .map {
                          // transaction could've been retried, simply ignore this record in this case
                          case UpdateLockResult.OffsetAlreadyCommitted =>
                            false
                          // other consumer already took lock of this source
                          case UpdateLockResult.OutdatedLock | UpdateLockResult.OffsetTooOld =>
                            stopSource.invoke(l.key.partition)
                            false
                          case UpdateLockResult.Success =>
                            true
                        }
                    pubsub.ConsumerRecord(
                      data = record.userData,
                      partitionNumber = record.topicKey.partition,
                      commit = commitAction)
                  }
                  .viaMat(KillSwitches.single[ConsumerRecord[KeyValue]])(Keep.right)
                  .preMaterialize()(materializer)
              new StreamData(killSwitch, source, l, false)
            }
        existingStreams = toLeave ++ newStreams
        if (newStreams.nonEmpty) onPull()
      }

      private def stopSource = getAsyncCallback[Int] { partitionNumber =>
        val (toShutdown, toLeave) =
          existingStreams.partition(_.lock.key.partition == partitionNumber)
        existingStreams = toLeave
        toShutdown.foreach(_.killSwitch.shutdown())
      }

      private def existingLocksForPartitions(): Map[Int, ConsumerLock] = {
        existingStreams.map { streamData =>
          streamData.lock.key.partition -> streamData.lock
        }.toMap
      }

    }
  }
}
