package com.github.pwliwanow.fdb.pubsub.internal.common

import java.time.Instant

import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.Versionstamp

private[pubsub] final case class TopicKey(topic: String, partition: Int, versionstamp: Versionstamp)

private[pubsub] final case class TopicRecord(
    topicKey: TopicKey,
    userData: KeyValue,
    createdAt: Instant)
