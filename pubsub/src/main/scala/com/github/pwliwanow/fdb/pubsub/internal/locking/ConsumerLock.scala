package com.github.pwliwanow.fdb.pubsub.internal.locking

import java.time.Instant
import java.util.UUID

import com.apple.foundationdb.tuple.Versionstamp

private[pubsub] final case class ConsumerLockKey(
    topic: String,
    consumerGroup: String,
    partition: Int)

private[pubsub] final case class ConsumerLock(
    key: ConsumerLockKey,
    acquiredWith: Versionstamp,
    refreshedAt: Instant,
    acquiredBy: UUID)
