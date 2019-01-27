package com.github.pwliwanow.fdb.pubsub.javadsl
import java.util.concurrent.CompletableFuture

import akka.NotUsed
import com.apple.foundationdb.TransactionContext
import com.github.pwliwanow.fdb.pubsub.scaladsl.{Producer => ScalaProducer}

import scala.compat.java8.FutureConverters._

protected class FdbProducer(underlying: ScalaProducer) extends Producer {

  override def send(
      tx: TransactionContext,
      topic: String,
      key: Array[Byte],
      value: Array[Byte]): CompletableFuture[NotUsed] = {
    implicit val ec = fromExecutor(tx.getExecutor)
    underlying.send(topic, key, value).transact(tx).toJava.toCompletableFuture
  }

  override def send(
      tx: TransactionContext,
      topic: String,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): CompletableFuture[NotUsed] = {
    implicit val ec = fromExecutor(tx.getExecutor)
    underlying.send(topic, key, value, userVersion).transact(tx).toJava.toCompletableFuture
  }

  override def send(
      tx: TransactionContext,
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte]): CompletableFuture[NotUsed] = {
    implicit val ec = fromExecutor(tx.getExecutor)
    underlying.send(topic, partitionNumber, key, value).transact(tx).toJava.toCompletableFuture
  }

  override def send(
      tx: TransactionContext,
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): CompletableFuture[NotUsed] = {
    implicit val ec = fromExecutor(tx.getExecutor)
    underlying
      .send(topic, partitionNumber, key, value, userVersion)
      .transact(tx)
      .toJava
      .toCompletableFuture
  }
}
