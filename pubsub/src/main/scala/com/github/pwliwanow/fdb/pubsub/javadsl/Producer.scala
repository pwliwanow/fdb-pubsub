package com.github.pwliwanow.fdb.pubsub.javadsl
import java.util.concurrent.CompletableFuture

import akka.NotUsed
import com.apple.foundationdb.TransactionContext

trait Producer {

  /** It will result in failed [[java.util.concurrent.CompletableFuture]] with
    * [[java.util.concurrent.CompletionException]] caused by
    * [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    * if given topic does not exists.
    */
  def send(
      tx: TransactionContext,
      topic: String,
      key: Array[Byte],
      value: Array[Byte]): CompletableFuture[NotUsed]

  /** It will result in failed [[CompletableFuture]] with
    * [[java.util.concurrent.CompletionException]] caused by
    * [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    * if given topic does not exists.
    */
  def send(
      tx: TransactionContext,
      topic: String,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): CompletableFuture[NotUsed]

  /** It will result in failed [[CompletableFuture]] with
    * [[java.util.concurrent.CompletionException]] caused by:
    * - [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    *   if given topic does not exists.
    * - [[com.github.pwliwanow.fdb.pubsub.error.PartitionNotExistsException]]
    *   if specified partition does not exist.
    */
  def send(
      tx: TransactionContext,
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte]): CompletableFuture[NotUsed]

  /** It will result in failed [[CompletableFuture]] with
    * [[java.util.concurrent.CompletionException]] caused by:
    * - [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    *   if given topic does not exists.
    * - [[com.github.pwliwanow.fdb.pubsub.error.PartitionNotExistsException]]
    *   if specified partition does not exist.
    */
  def send(
      tx: TransactionContext,
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): CompletableFuture[NotUsed]

}
