package com.github.pwliwanow.fdb.pubsub
import java.time.Instant

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

}
