package com.github.pwliwanow.fdb.pubsub.javadsl

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.javadsl.SubSource
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.{Database, KeyValue}
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
  def producer(): Producer
}

protected class FdbPubSubClient(underlying: ScalaPubSubClient) extends PubSubClient {
  private val javaProducer = new FdbProducer(underlying.producer)

  override def createTopic(
      topic: String,
      numberOfPartitions: Int,
      ec: ExecutionContextExecutor): CompletionStage[NotUsed] = {
    underlying.createTopic(topic, numberOfPartitions)(ec).toJava
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
}
