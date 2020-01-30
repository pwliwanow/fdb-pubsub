package com.github.pwliwanow.fdb.pubsub.javadsl

import java.util
import java.util.Optional
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.{Done, NotUsed}
import akka.stream.Materializer
import akka.stream.javadsl.SubSource
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Versionstamp
import com.apple.foundationdb.{Database, KeyValue, ReadTransactionContext, TransactionContext}
import com.github.pwliwanow.fdb.pubsub.scaladsl.{PubSubClient => ScalaPubSubClient}
import com.github.pwliwanow.fdb.pubsub.{ConsumerRecord, ConsumerSettings}

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContextExecutor

object PubSubClient {
  def create(pubSubSubspace: Subspace, database: Database): PubSubClient = {
    val underlying = ScalaPubSubClient(pubSubSubspace, database)
    new FdbPubSubClient(underlying)
  }
}

abstract class PubSubClient {

  /** If topic already exists with different number of partitions that specified
    * this will fail with [[java.util.concurrent.CompletionException]] caused by
    * [[com.github.pwliwanow.fdb.pubsub.error.TopicAlreadyExistsException]].
    */
  def createTopic(
      topic: String,
      numberOfPartitions: Int,
      ec: ExecutionContextExecutor): CompletionStage[NotUsed]

  def consumer(
      topic: String,
      consumerGroup: String,
      settings: ConsumerSettings,
      mat: Materializer): SubSource[ConsumerRecord[KeyValue], NotUsed]

  /** Clears partition for topic up to provided [[Versionstamp]].
    *
    * Use with caution, as it cannot be reverted and data will be deleted.
    */
  def clear(
      tx: TransactionContext,
      topic: String,
      partitionNumber: Int,
      untilOffset: Versionstamp): CompletableFuture[Done]

  /** Returns latest committed offset for the given topic, consumer group and partition number */
  def getOffset(
      tx: ReadTransactionContext,
      topic: String,
      consumerGroup: String,
      partitionNumber: Int): CompletionStage[Optional[Versionstamp]]

  /** Returns partition indices that exist for this topic. */
  def getPartitions(tx: ReadTransactionContext, topic: String): CompletionStage[util.List[Int]]

  def producer(): Producer
}

protected class FdbPubSubClient(underlying: ScalaPubSubClient) extends PubSubClient {
  private val javaProducer = new FdbProducer(underlying.producer)

  override def createTopic(
      topic: String,
      numberOfPartitions: Int,
      ec: ExecutionContextExecutor): CompletionStage[NotUsed] = {
    underlying.createTopic(topic, numberOfPartitions)(ec).toJava.thenApply(_ => NotUsed)
  }

  override def producer() = javaProducer

  def consumer(
      topic: String,
      consumerGroup: String,
      settings: ConsumerSettings,
      mat: Materializer): SubSource[ConsumerRecord[KeyValue], NotUsed] = {
    val scalaConsumer = underlying.consumer(topic, consumerGroup, settings)(mat)
    new SubSource[ConsumerRecord[KeyValue], NotUsed](scalaConsumer)
  }

  /** Clears partition for topic up to provided [[Versionstamp]].
    *
    * Use with caution, as it cannot be reverted and data will be deleted.
    */
  override def clear(
      tx: TransactionContext,
      topic: String,
      partitionIndex: Int,
      untilOffset: Versionstamp): CompletableFuture[Done] = {
    underlying.clear(topic, partitionIndex, untilOffset).transactJava(tx).thenApply(_ => Done)
  }

  /** Returns latest committed offset for the given topic, consumer group and partition number */
  override def getOffset(
      tx: ReadTransactionContext,
      topic: String,
      consumerGroup: String,
      partitionNo: Int): CompletionStage[Optional[Versionstamp]] = {
    underlying
      .getOffset(topic, consumerGroup, partitionNo)
      .map(_.fold(Optional.empty[Versionstamp]())(Optional.of))
      .transactJava(tx)
  }

  /** Returns partition indices that exist for this topic. */
  override def getPartitions(
      tx: ReadTransactionContext,
      topic: String): CompletionStage[util.List[Int]] = {
    underlying
      .getPartitions(topic)
      .map { xs =>
        val result: util.List[Int] = new util.ArrayList[Int]()
        xs.iterator.foreach(result.add)
        result
      }
      .transactJava(tx)
  }
}
