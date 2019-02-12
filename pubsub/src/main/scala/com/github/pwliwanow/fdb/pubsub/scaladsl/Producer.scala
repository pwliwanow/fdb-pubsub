package com.github.pwliwanow.fdb.pubsub.scaladsl

import java.time.Instant
import java.util

import akka.NotUsed
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.Versionstamp
import com.github.pwliwanow.fdb.pubsub.error.{PartitionNotExistsException, TopicNotExistsException}
import com.github.pwliwanow.fdb.pubsub.internal.common.{TopicKey, TopicRecord, TopicSubspace}
import com.github.pwliwanow.fdb.pubsub.internal.metadata.TopicMetadataService
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO}

trait Producer {

  /** It will result in failed [[DBIO]] with
    * [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    * if given topic does not exists.
    */
  def send(topic: String, key: Array[Byte], value: Array[Byte]): DBIO[NotUsed]

  /** It will result in failed [[DBIO]] with
    * [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    * if given topic does not exists.
    */
  def send(topic: String, key: Array[Byte], value: Array[Byte], userVersion: Int): DBIO[NotUsed]

  /** It will result in failed [[DBIO]] with:
    * - [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    *   if given topic does not exists.
    * - [[com.github.pwliwanow.fdb.pubsub.error.PartitionNotExistsException]]
    *   if specified partition does not exist.
    */
  def send(topic: String, partitionNumber: Int, key: Array[Byte], value: Array[Byte]): DBIO[NotUsed]

  /** It will result in failed [[DBIO]] with:
    * - [[com.github.pwliwanow.fdb.pubsub.error.TopicNotExistsException]]
    *   if given topic does not exists.
    * - [[com.github.pwliwanow.fdb.pubsub.error.PartitionNotExistsException]]
    *   if specified partition does not exist.
    */
  def send(
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): DBIO[NotUsed]
}

private[pubsub] final class FdbProducer(
    subspace: TopicSubspace,
    metadataService: TopicMetadataService)
    extends Producer {

  def send(topic: String, key: Array[Byte], value: Array[Byte]): DBIO[NotUsed] = {
    send(topic, key, value, userVersion = 0)
  }

  def send(topic: String, key: Array[Byte], value: Array[Byte], userVersion: Int): DBIO[NotUsed] = {
    for {
      numberOfPartitions <- getNumberOfPartitions(topic).toDBIO
      partitionNumber = Math.abs(util.Arrays.hashCode(key) % numberOfPartitions)
      _ <- store(topic, partitionNumber, key, value, userVersion)
    } yield NotUsed
  }

  def send(
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte]): DBIO[NotUsed] = {
    send(topic, partitionNumber, key, value, userVersion = 0)
  }

  def send(
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): DBIO[NotUsed] = {
    for {
      numberOfPartitions <- getNumberOfPartitions(topic).toDBIO
      _ <- if (numberOfPartitions < partitionNumber) {
        DBIO.failed(PartitionNotExistsException(partitionNumber, numberOfPartitions))
      } else {
        store(topic, partitionNumber, key, value, userVersion)
      }
    } yield NotUsed
  }

  private def store(
      topic: String,
      partitionNumber: Int,
      key: Array[Byte],
      value: Array[Byte],
      userVersion: Int): DBIO[Unit] = {
    val topicKey = TopicKey(topic, partitionNumber, Versionstamp.incomplete(userVersion))
    val userData = new KeyValue(key, value)
    val record = TopicRecord(topicKey, userData, Instant.now)
    subspace.set(record)
  }

  private def getNumberOfPartitions(topic: String): ReadDBIO[Int] = {
    metadataService
      .getNumberOfPartitions(topic)
      .flatMap(_.map(ReadDBIO.pure).getOrElse(ReadDBIO.failed(TopicNotExistsException(topic))))
  }
}
