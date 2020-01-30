package com.github.pwliwanow.fdb.pubsub.javadsl

import java.util.concurrent.CompletionException

import akka.NotUsed
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.FdbPubSubSpec
import com.github.pwliwanow.fdb.pubsub.error.PartitionNotExistsException

import scala.compat.java8.FutureConverters._

class ProducerSpec extends FdbPubSubSpec {

  lazy val javaProducer = new FdbProducer(pubSubClient.producer)

  behavior of "JavaProducer"

  it should "send records to the specified topic in correct order for the same partition" in new TopicMetadataFixture {
    val partitionNumber = 0
    val (keys, values) = createKeyValues(3).unzip
    database
      .runAsync { tx =>
        val futureResult = javaProducer.send(
          tx,
          topic,
          partitionNumber,
          Tuple.from(keys(1)).pack,
          Tuple.from(values(1)).pack,
          userVersion = 2)
        futureResult.thenCompose[NotUsed] { _ =>
          javaProducer.send(
            tx,
            topic,
            partitionNumber,
            Tuple.from(keys.head).pack,
            Tuple.from(values.head).pack)
        }
      }
      .join()
    database
      .runAsync { tx =>
        javaProducer
          .send(tx, topic, partitionNumber, Tuple.from(keys(2)).pack, Tuple.from(values(2)).pack)
      }
      .join()
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
        database
          .runAsync { tx =>
            javaProducer
              .send(tx, topic, Tuple.from(key).pack, Tuple.from(value).pack)
          }
          .toScala
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
    val produce = () =>
      database.runAsync { tx =>
        javaProducer.send(
          tx,
          topic,
          tooHighPartitionNumber,
          Tuple.from(keys.head).pack,
          Tuple.from(values.head).pack,
          2)
      }
    val exception = intercept[CompletionException](produce().join())
    assert(exception.getCause.isInstanceOf[PartitionNotExistsException])
  }

}
