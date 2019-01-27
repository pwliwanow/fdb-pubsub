package com.github.pwliwanow.fdb.pubsub.scaladsl

import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.FdbPubSubSpec
import com.github.pwliwanow.fdb.pubsub.error.PartitionNotExistsException

class ProducerSpec extends FdbPubSubSpec {

  it should "send records to the specified topic in correct order for the same partition" in new TopicMetadataFixture {
    val partitionNumber = 0
    val (keys, values) = createKeyValues(3).unzip
    val dbio = for {
      _ <- producer.send(
        topic,
        partitionNumber,
        Tuple.from(keys(1)).pack,
        Tuple.from(values(1)).pack,
        userVersion = 2)
      _ <- producer.send(
        topic,
        partitionNumber,
        Tuple.from(keys.head).pack,
        Tuple.from(values.head).pack)
    } yield ()
    dbio.transact(database).await
    producer
      .send(topic, partitionNumber, Tuple.from(keys(2)).pack, Tuple.from(values(2)).pack)
      .transact(database)
      .await

    val records =
      typedTopicSubspace.getRange(typedTopicSubspace.range()).transact(database).await.toList
    val keysInDb = records.map(readKey)
    val valuesInDb = records.map(readValue)
    assert(keysInDb === keys)
    assert(valuesInDb === values)
  }

  it should "redistribute data to different partitions" in new TopicMetadataFixture {
    val partitionsWithKeyValues =
      createKeyValues(3).map {
        case (k, v) => (expectedPartition(k), k, v)
      }
    partitionsWithKeyValues.foreach {
      case (_, key, value) =>
        producer
          .send(topic, Tuple.from(key).pack, Tuple.from(value).pack)
          .transact(database)
          .await
    }
    val recordsInDb =
      typedTopicSubspace
        .getRange(typedTopicSubspace.range())
        .transact(database)
        .await
        .toList
        .map(r => (r.topicKey.partition, readKey(r), readValue(r)))
    assert(recordsInDb === partitionsWithKeyValues.sorted)
  }

  it should "fail if client tries to send data to a partition number that too high" in new TopicMetadataFixture {
    val tooHighPartitionNumber = 20
    val (keys, values) = createKeyValues(3).unzip
    val dbio =
      producer.send(
        topic,
        tooHighPartitionNumber,
        Tuple.from(keys.head).pack,
        Tuple.from(values.head).pack,
        2)
    assertThrows[PartitionNotExistsException](dbio.transact(database).await)
  }

}
