package com.github.pwliwanow.fdb.pubsub.internal.metadata

import java.time.Instant

import com.apple.foundationdb.tuple.Versionstamp

private[pubsub] case class TopicMetadata(
    topic: String,
    numberOfPartitions: Int,
    createdAt: Instant,
    createdIn: Versionstamp)
