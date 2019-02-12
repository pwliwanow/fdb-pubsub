package com.github.pwliwanow.fdb.pubsub.javadsl
import java.util.concurrent.CompletableFuture

import akka.NotUsed
import com.apple.foundationdb.TransactionContext
import com.github.pwliwanow.fdb.pubsub.scaladsl.{Producer => ScalaProducer}

protected class FdbProducer(underlying: ScalaProducer) extends Producer {

  override def send(
      tx: TransactionContext,
      topic: String,
      key: Array[Byte],
      value: Array[Byte]): CompletableFuture[NotUsed] = {
    underlying.send(topic, key, value).transactJava(tx)
  }

  override def send(
      tx: TransactionContext,
      topic: String,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): CompletableFuture[NotUsed] = {
    underlying.send(topic, key, value, userVersion).transactJava(tx)
  }

  override def send(
      tx: TransactionContext,
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte]): CompletableFuture[NotUsed] = {
    underlying.send(topic, partitionNumber, key, value).transactJava(tx)
  }

  override def send(
      tx: TransactionContext,
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): CompletableFuture[NotUsed] = {
    underlying.send(topic, partitionNumber, key, value, userVersion).transactJava(tx)
  }
}
