package com.github.pwliwanow.fdb.pubsub.internal.metadata
import com.apple.foundationdb.tuple.Versionstamp
import com.github.pwliwanow.fdb.pubsub.FdbPubSubSpec

class ConsumerGroupMetadataSubspaceSpec extends FdbPubSubSpec {

  it should "be able to serialize and then deserialize the value" in {
    val value =
      ConsumerGroupMetadata(
        key = ConsumerGroupMetadataKey("testTopic", "consumerGroup", 5),
        offset = Versionstamp.complete(Array.fill[Byte](10)(0x0: Byte), 10),
        updatedWithLock = Versionstamp.complete(Array.fill[Byte](10)(0x0: Byte), 2)
      )
    typedConsumerGroupMetadataSubspace.set(value).transact(database).await
    val fromDb =
      typedConsumerGroupMetadataSubspace
        .getRange(typedConsumerGroupMetadataSubspace.range())
        .transact(database)
        .await
    assert(fromDb === List(value))
  }

}
