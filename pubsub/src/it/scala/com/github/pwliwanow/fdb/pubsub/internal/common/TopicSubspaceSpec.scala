package com.github.pwliwanow.fdb.pubsub.internal.common
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.github.pwliwanow.fdb.pubsub.FdbPubSubSpec

class TopicSubspaceSpec extends FdbPubSubSpec {

  it should "be able to serialize and then deserialize the value" in {
    val userKey = Tuple.from("key").pack()
    val userValue = Tuple.from("value").pack()
    val versionstamp = Versionstamp.complete(Array.fill[Byte](10)(0x0: Byte))
    val value =
      TopicRecord(
        topicKey = TopicKey("testTopic", 3, versionstamp),
        userData = new KeyValue(userKey, userValue),
        createdAt = utcClock.instant())
    typedTopicSubspace.set(value).transact(database).await
    val fromDb =
      typedTopicSubspace
        .getRange(typedTopicSubspace.range)
        .transact(database)
        .await
    assert(fromDb === List(value))
  }

}
