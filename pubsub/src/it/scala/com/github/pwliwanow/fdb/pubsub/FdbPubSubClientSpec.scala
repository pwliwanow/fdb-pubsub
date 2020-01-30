package com.github.pwliwanow.fdb.pubsub
import java.time.Instant

import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.github.pwliwanow.fdb.pubsub.error.TopicAlreadyExistsException

import scala.concurrent.duration._

class FdbPubSubClientSpec extends FdbPubSubSpec {

  it should "correctly create topic" in {
    val before = Instant.now
    val tolerance = 100.millis
    pubSubClient.createTopic(topic, numberOfPartitions).await
    val after = Instant.now
    val instantOrdering = Ordering.by[Instant, Long](_.toEpochMilli)
    import instantOrdering._
    val metadata = typedTopicMetadataSubspace.get(topic).transact(database).await.get
    assert(metadata.numberOfPartitions === numberOfPartitions)
    assert(before.minusMillis(tolerance.toMillis) <= metadata.createdAt)
    assert(metadata.createdAt <= after.plusMillis(tolerance.toMillis))
  }

  it should "throw exception if topic already exists" in {
    pubSubClient.createTopic(topic, numberOfPartitions).await
    assertThrows[TopicAlreadyExistsException] {
      pubSubClient.createTopic(topic, numberOfPartitions + 1).await
    }
  }

  it should "complete normally if topic already exist with the same number of partitions as provided" in {
    pubSubClient.createTopic(topic, numberOfPartitions).await
    pubSubClient.createTopic(topic, numberOfPartitions).await
  }

  it should "clear topic partition until given offset" in {
    pubSubClient.createTopic(topic, 2).await
    val partitionNumber = 1
    val (toClear, toLeave) = createKeyValues(6).splitAt(4)
    val maybeVersionstamp =
      toClear.foldLeft(Option.empty[Versionstamp]) {
        case (_, (k, v)) => produceVersionstamped(k, v, partitionNumber)
      }
    toLeave.foreach { case (k, v) => produceVersionstamped(k, v, partitionNumber) }

    pubSubClient.clear(topic, partitionNumber, maybeVersionstamp.get).transact(database).await

    val result =
      typedTopicSubspace
        .getRange(typedTopicSubspace.range())
        .transact(database)
        .await
        .map { r =>
          (readString(r.userData.getKey), readString(r.userData.getValue))
        }
    assert(result.toList === toLeave)
  }

  it should "return correct partitions" in {
    pubSubClient.createTopic(topic, 5).await
    val partitions = pubSubClient.getPartitions(topic).transact(database).await
    assert(partitions === (0 to 4).toList)
  }

  private def produceVersionstamped(
      k: String,
      v: String,
      partitionNumber: Int): Option[Versionstamp] = {
    producer
      .send(topic, partitionNumber, Tuple.from(k).pack, Tuple.from(v).pack)
      .transactVersionstamped(database)
      .await
      ._2
  }

  private def readString(bytes: Array[Byte]): String =
    Tuple.fromBytes(bytes).getString(0)
}
