package com.github.pwliwanow.fdb.pubsub.scaladsl

import java.time.{Clock, ZoneOffset}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, RunnableGraph, Sink, Source, SubFlow}
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.apple.foundationdb.{Database, KeyValue}
import com.github.pwliwanow.fdb.pubsub.internal.common.{SerDe, TopicMetadataSubspace, TopicSubspace}
import com.github.pwliwanow.fdb.pubsub.internal.consumer._
import com.github.pwliwanow.fdb.pubsub.internal.locking.{ConsumerLockService, ConsumersLockSubspace}
import com.github.pwliwanow.fdb.pubsub.internal.metadata.{
  ConsumerGroupMetadataService,
  ConsumerGroupMetadataSubspace,
  TopicMetadataService
}
import com.github.pwliwanow.fdb.pubsub.{ConsumerRecord, ConsumerSettings}
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO}

import scala.concurrent.{ExecutionContextExecutor, Future}

object PubSubClient {
  import com.github.pwliwanow.fdb.pubsub.internal.SubspaceNames._

  type SourceRepr[+O] = Source[O, NotUsed]

  type ConsumerSubSource =
    SubFlow[ConsumerRecord[KeyValue], NotUsed, SourceRepr, RunnableGraph[NotUsed]]

  def apply(pubSubSubspace: Subspace, database: Database): PubSubClient = {
    val topicSubspace = pubSubSubspace.subspace(Tuple.from(TopicSubspaceName))
    val consumerGroupMetadataSubspace =
      pubSubSubspace.subspace(Tuple.from(ConsumerGroupMetadataSubspaceName))
    val lockSubspace = pubSubSubspace.subspace(Tuple.from(LocksSubspaceName))
    val topicMetadataSubspace = pubSubSubspace.subspace(Tuple.from(TopicMetadataSubspaceName))
    create(
      topicSubspace = topicSubspace,
      consumerGroupMetadataSubspace = consumerGroupMetadataSubspace,
      lockSubspace = lockSubspace,
      topicMetadataSubspace = topicMetadataSubspace,
      database = database
    )
  }

  private def create(
      topicSubspace: Subspace,
      consumerGroupMetadataSubspace: Subspace,
      lockSubspace: Subspace,
      topicMetadataSubspace: Subspace,
      database: Database): PubSubClient = {
    val clock = Clock.tickMillis(ZoneOffset.UTC)
    val typedTopicSubspace = new TopicSubspace(topicSubspace)
    val typedMetadataSubspace = new TopicMetadataSubspace(topicMetadataSubspace)
    val metadataService = new TopicMetadataService(typedMetadataSubspace)
    val typedConsumerGroupMetadataSubspace = new ConsumerGroupMetadataSubspace(
      consumerGroupMetadataSubspace)
    val consumerGroupMetadataService = new ConsumerGroupMetadataService(
      typedConsumerGroupMetadataSubspace)
    val consumerService =
      new ConsumerService(consumerGroupMetadataService, typedTopicSubspace, database)
    val typedLockSubspace = new ConsumersLockSubspace(lockSubspace)
    new FdbPubSubClient(
      clock,
      typedTopicSubspace,
      consumerGroupMetadataService,
      metadataService,
      consumerService,
      typedLockSubspace,
      database)
  }
}

trait PubSubClient {

  /** If topic already exists with different number of partitions that specified
    * this will fail with [[com.github.pwliwanow.fdb.pubsub.error.TopicAlreadyExistsException]].
    */
  def createTopic(topic: String, numberOfPartitions: Int)(
      implicit ec: ExecutionContextExecutor): Future[Unit]

  def consumer(topic: String, consumerGroup: String, settings: ConsumerSettings)(
      implicit mat: Materializer): PubSubClient.ConsumerSubSource

  /** Clears partition for topic up to provided [[Versionstamp]].
    *
    * Use with caution, as it cannot be reverted and data will be deleted.
    */
  def clear(topic: String, partitionNumber: Int, untilOffset: Versionstamp): DBIO[Unit]

  /** Returns latest committed offset for the given topic, consumer group and partition number */
  def getOffset(
      topic: String,
      consumerGroup: String,
      partitionNumber: Int): ReadDBIO[Option[Versionstamp]]

  /** Returns partition indices that exist for this topic. */
  def getPartitions(topic: String): ReadDBIO[List[Int]]

  def producer: Producer
}

private[pubsub] class FdbPubSubClient(
    private val clock: Clock,
    private val topicSubspace: TopicSubspace,
    private val consumerGroupMetadataService: ConsumerGroupMetadataService,
    private val topicMetadataService: TopicMetadataService,
    private val consumerService: ConsumerService,
    private val lockSubspace: ConsumersLockSubspace,
    private val database: Database)
    extends PubSubClient {

  override val producer: Producer = {
    new FdbProducer(topicSubspace, topicMetadataService)
  }

  override def createTopic(topic: String, numberOfPartitions: Int)(
      implicit ec: ExecutionContextExecutor): Future[Unit] = {
    topicMetadataService.createTopic(topic, numberOfPartitions).transact(database)
  }

  override def consumer(topic: String, consumerGroup: String, settings: ConsumerSettings)(
      implicit mat: Materializer): PubSubClient.ConsumerSubSource = {
    val lockService = new ConsumerLockService(lockSubspace, clock, settings.lockValidityDuration)
    val lockAcquirer =
      new ConsumerPartitionLockAcquirer(lockService, lockSubspace, topicMetadataService, database)
    val subscriptionDetails =
      SubscriptionDetails(topic, consumerGroup, settings.partitionPollingInterval)
    val partitionSourcesSource =
      new PartitionSources(
        consumerService,
        lockAcquirer,
        subscriptionDetails,
        settings.acquireLocksInitialDelay,
        settings.acquireLocksInterval)
    val source = Source.fromGraph(partitionSourcesSource)
    val merge = new MergeBack[ConsumerRecord[KeyValue], source.Repr, NotUsed] {
      override def apply[T](
          flow: Flow[ConsumerRecord[KeyValue], T, NotUsed],
          breadth: Int): source.Repr[T] = {
        source.map(_.via(flow)).flatMapMerge(breadth, identity)
      }
    }
    val finish: Sink[ConsumerRecord[KeyValue], NotUsed] => source.Closed = s =>
      source.to(Sink.foreach { source =>
        source.runWith(s)
        ()
      })
    new SubFlowImpl(Flow[ConsumerRecord[KeyValue]], merge, finish)
  }

  override def clear(topic: String, partitionNumber: Int, upTo: Versionstamp): DBIO[Unit] = {
    // topic, partition, versionstamp
    val startRange = topicSubspace.range(Tuple.from(topic, SerDe.encodeInt(partitionNumber)))
    val endRange = topicSubspace.range(Tuple.from(topic, SerDe.encodeInt(partitionNumber), upTo))
    val range = new com.apple.foundationdb.Range(startRange.begin, endRange.begin)
    topicSubspace.clear(range)
  }

  override def getOffset(topic: String, consumerGroup: String, partitionNumber: Int) = {
    consumerGroupMetadataService.getOffset(topic, consumerGroup, partitionNumber)
  }

  override def getPartitions(topic: String): ReadDBIO[List[Int]] = {
    topicMetadataService
      .getNumberOfPartitions(topic)
      .map(_.fold(List.empty[Int])(n => (0 until n).toList))
  }
}
