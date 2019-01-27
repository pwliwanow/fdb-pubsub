package com.github.pwliwanow.fdb.pubsub.error

abstract class PubSubException(message: String) extends RuntimeException(message)

final case class TopicNotExistsException(topic: String)
    extends PubSubException(s"Topic $topic does not exists")

object TopicAlreadyExistsException {
  private def errorMsg(
      topic: String,
      providedNumberOfPartitions: Int,
      existingNumberOfPartitions: Int): String = {
    s"Topic [$topic] already exists with $existingNumberOfPartitions" +
      s" instead of provided $providedNumberOfPartitions"
  }
}

final case class TopicAlreadyExistsException(
    topic: String,
    providedNumberOfPartitions: Int,
    existingNumberOfPartitions: Int)
    extends PubSubException(
      TopicAlreadyExistsException
        .errorMsg(topic, providedNumberOfPartitions, existingNumberOfPartitions))

object PartitionNotExistsException {
  private def errorMsg(providedPartitionNumber: Int, numberOfPartitions: Int): String = {
    s"Provided partition number [$providedPartitionNumber] does not exists. " +
      s"Total number of partitions for this topic [$numberOfPartitions]."
  }
}

final case class PartitionNotExistsException(providedPartitionNumber: Int, numberOfPartitions: Int)
    extends PubSubException(
      PartitionNotExistsException.errorMsg(providedPartitionNumber, numberOfPartitions))
