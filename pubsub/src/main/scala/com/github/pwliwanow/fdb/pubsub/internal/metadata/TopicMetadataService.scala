package com.github.pwliwanow.fdb.pubsub.internal.metadata

import java.time.Instant

import akka.NotUsed
import com.apple.foundationdb.tuple.Versionstamp
import com.github.pwliwanow.fdb.pubsub.error.TopicAlreadyExistsException
import com.github.pwliwanow.fdb.pubsub.internal.common.TopicMetadataSubspace
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO}

private[pubsub] class TopicMetadataService(subspace: TopicMetadataSubspace) {

  /** If topic already exists with different number of partitions that specified
    * this will fail with [[TopicAlreadyExistsException]].
    */
  def createTopic(topic: String, numberOfPartitions: Int): DBIO[NotUsed] = {
    val metadata = TopicMetadata(topic, numberOfPartitions, Instant.now, Versionstamp.incomplete())
    for {
      maybeExistingMetadata <- subspace.get(topic): DBIO[Option[TopicMetadata]]
      _ <- maybeExistingMetadata.fold(subspace.set(metadata)) { alreadyExisting =>
        if (alreadyExisting.numberOfPartitions == numberOfPartitions) DBIO.pure(())
        else
          DBIO.failed(
            TopicAlreadyExistsException(
              topic,
              numberOfPartitions,
              alreadyExisting.numberOfPartitions))
      }
    } yield NotUsed
  }

  def getNumberOfPartitions(topic: String): ReadDBIO[Option[Int]] = {
    subspace.get(topic).map(maybeMetadata => maybeMetadata.map(_.numberOfPartitions))
  }

}
