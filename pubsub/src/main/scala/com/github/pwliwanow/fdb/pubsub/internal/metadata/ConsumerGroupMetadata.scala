package com.github.pwliwanow.fdb.pubsub.internal.metadata

import com.apple.foundationdb.tuple.Versionstamp

private[pubsub] case class ConsumerGroupMetadataKey(
    topic: String,
    consumerGroup: String,
    partitionNo: Int)

private[pubsub] case class ConsumerGroupMetadata(
    key: ConsumerGroupMetadataKey,
    offset: Versionstamp,
    updatedWithLock: Versionstamp)
