package com.github.pwliwanow.fdb.pubsub.internal.common
import com.apple.foundationdb.tuple.Versionstamp
import com.github.pwliwanow.fdb.pubsub.FdbPubSubSpec
import com.github.pwliwanow.fdb.pubsub.internal.metadata.TopicMetadata

class TopicMetadataSubspaceSpec extends FdbPubSubSpec {

  it should "be able to serialize and then deserialize the value" in {
    val versionstamp = Versionstamp.complete(Array.fill[Byte](10)(0x0: Byte))
    val value =
      TopicMetadata(
        topic = "testTopic",
        numberOfPartitions = 7,
        createdAt = utcClock.instant(),
        createdIn = versionstamp)
    typedTopicMetadataSubspace.set(value).transact(database).await
    val fromDb =
      typedTopicMetadataSubspace
        .getRange(typedTopicMetadataSubspace.range)
        .transact(database)
        .await
    assert(fromDb === List(value))
  }

}
