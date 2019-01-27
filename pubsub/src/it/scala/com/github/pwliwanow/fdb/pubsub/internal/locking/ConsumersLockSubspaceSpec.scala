package com.github.pwliwanow.fdb.pubsub.internal.locking
import java.util.UUID

import com.apple.foundationdb.tuple.Versionstamp
import com.github.pwliwanow.fdb.pubsub.FdbPubSubSpec

class ConsumersLockSubspaceSpec extends FdbPubSubSpec {

  it should "be able to serialize and then deserialize the value" in {
    val versionstamp = Versionstamp.complete(Array.fill[Byte](10)(0x0: Byte))
    val value =
      ConsumerLock(
        key = ConsumerLockKey("testTopic", "consGroup", 2),
        acquiredWith = versionstamp,
        refreshedAt = utcClock.instant(),
        acquiredBy = UUID.randomUUID)
    typedLockSubspace.set(value).transact(database).await
    val fromDb =
      typedLockSubspace
        .getRange(typedLockSubspace.range)
        .transact(database)
        .await
    assert(fromDb === List(value))
  }

}
