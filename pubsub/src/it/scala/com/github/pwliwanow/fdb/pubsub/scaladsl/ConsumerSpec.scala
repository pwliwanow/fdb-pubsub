package com.github.pwliwanow.fdb.pubsub.scaladsl

import java.time.Instant
import java.util.UUID

import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, MergeHub, Sink}
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.github.pwliwanow.fdb.pubsub.internal.locking.{ConsumerLock, ConsumerLockKey}
import com.github.pwliwanow.fdb.pubsub.internal.metadata._
import com.github.pwliwanow.fdb.pubsub.{ConsumerRecord, ConsumerSettings, FdbPubSubSpec}
import com.github.pwliwanow.foundationdb4s.core.DBIO

import scala.concurrent.Future
import scala.concurrent.duration._

class ConsumerSpec extends FdbPubSubSpec {

  val consumerGroup = "testConsumerGroup"

  it should "stream data in order from single partition" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 10
      val keyValues = createKeyValues(numberOfElements)
      val partitionNumber = 0
      insertKeyValuesToPartition(keyValues, partitionNumber)
      val consumer = createConsumer()

      val result = consumer.mergeSubstreams
        .take(numberOfElements.toLong)
        .runWith(Sink.seq)
        .await
        .toList
      val obtainedData = result.map(r => (r.partitionNumber, readKey(r), readValue(r)))
      val expectedData = keyValues.map { case (k, v) => (partitionNumber, k, v) }
      assert(obtainedData === expectedData)
    }
  }

  it should "continuously stream data" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 10
      val keyValues = createKeyValues(numberOfElements)
      val partitionNumber = 0
      val consumer = createConsumer()

      val futureResult = consumer.mergeSubstreams
        .take(numberOfElements.toLong)
        .runWith(Sink.seq)

      // allow to initializate all the components
      Thread.sleep(500)

      insertKeyValuesToPartition(keyValues, partitionNumber)
      val obtainedData = futureResult.await.map(r => (r.partitionNumber, readKey(r), readValue(r)))
      val expectedData = keyValues.map { case (k, v) => (partitionNumber, k, v) }
      assert(obtainedData === expectedData)
    }
  }

  it should "continuously stream data from multiple partitions" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 10
      val keyValues = createKeyValues(numberOfElements)
      val consumer = createConsumer()

      val futureResult = consumer.mergeSubstreams
        .take(numberOfElements.toLong)
        .runWith(Sink.seq)

      // allow to initializate all the components
      Thread.sleep(500)

      insertKeyValues(keyValues)
      val obtainedData =
        futureResult.await.map(r => (r.partitionNumber, readKey(r), readValue(r))).toList.sorted
      val expectedData = keyValues.map { case (k, v) => (expectedPartition(k), k, v) }.sorted
      assert(obtainedData === expectedData)
    }
  }

  it should "fetch and commit data using without using mergeSubstreams" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 10
      val partitionNumber = 4
      val keyValues = createKeyValues(numberOfElements)
      createConsumer().mergeSubstreams.to(Consumer.committableSink(database)).run()
      insertKeyValuesToPartition(keyValues, partitionNumber)
      val lastElement =
        typedTopicSubspace.getRange(typedTopicSubspace.range()).transact(database).await.last
      val checkCondition = () => {
        val consumerGroupMetadata =
          typedConsumerGroupMetadataSubspace
            .get(ConsumerGroupMetadataKey(topic, consumerGroup, partitionNumber))
            .transact(database)
            .await
            .get
        assert(consumerGroupMetadata.offset === lastElement.topicKey.versionstamp)
      }
      awaitAssert(checkCondition(), 2.seconds)
    }
  }

  it should "rebalance load after more consumers are created" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      case class RecordWithSource(source: String, consumerRecord: ConsumerRecord[KeyValue])

      val numberOfElements = 10
      val keyValues = createKeyValues(numberOfElements)
      val consumers =
        List(createConsumer(), createConsumer(), createConsumer()).zipWithIndex
          .map {
            case (subFlow, i) =>
              subFlow
                .via(Consumer.committableFlow(database))
                .map(r => RecordWithSource(source = s"consumer${i + 1}", r))
                .mergeSubstreams
          }

      val mergeSource = MergeHub.source[RecordWithSource]
      val (sink, futureResult) =
        mergeSource.take(numberOfElements.toLong).toMat(Sink.seq)(Keep.both).run()

      consumers.foreach { c =>
        c.runWith(sink)
        Thread.sleep(50)
      }
      Thread.sleep(2100)

      insertKeyValues(keyValues)

      val obtainedResult =
        futureResult.await.toList
          .map { sourceRecord =>
            (
              sourceRecord.source,
              readKey(sourceRecord.consumerRecord),
              sourceRecord.consumerRecord.partitionNumber)
          }
          .sortBy { case (s, k, p) => (k, p, s) }

      assert(
        keyValues.map(_._1) === obtainedResult.map(_._2),
        "Each element was consumed exactly once")

      obtainedResult
        .map { case (source, _, partitionNumber) => (source, partitionNumber) }
        .distinct
        .groupBy { case (_, partitionNo) => partitionNo }
        .foreach {
          case (partitionNumber, cons) =>
            assert(
              cons.size === 1,
              s"Each partition must be assigned to exactly one consumer. Partition number [$partitionNumber]")
        }
    }
  }

  it should "restart processing from the last committed message (each message was committed)" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 50
      val half = numberOfElements / 2
      val partitionNumber = 3
      val keyValues = createKeyValues(numberOfElements)
      val (firstHalf, secondHalf) = keyValues.splitAt(half)

      insertKeyValuesToPartition(firstHalf, partitionNumber)
      val commitableFlow = Consumer.committableFlow[KeyValue](database)
      val toResult: List[ConsumerRecord[KeyValue]] => List[(Int, String, String)] =
        _.map(r => (r.partitionNumber, readKey(r), readValue(r))).sorted
      val obtainedResult1 = toResult(runConsumerAndCollectData(half, commitableFlow))
      insertKeyValuesToPartition(secondHalf, partitionNumber)
      val obtainedResult2 = toResult(runConsumerAndCollectData(half, commitableFlow))

      val toExpectedResult: List[(String, String)] => List[(Int, String, String)] =
        _.map { case (k, v) => (partitionNumber, k, v) }
      val expectedResult1 = toExpectedResult(firstHalf)
      val expectedResult2 = toExpectedResult(secondHalf)
      assert(obtainedResult1 === expectedResult1)
      assert(obtainedResult2 === expectedResult2)
    }
  }

  it should "restart processing from the last committed message (no message was committed)" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 50
      val half = numberOfElements / 2
      val partitionNumber = 3
      val keyValues = createKeyValues(numberOfElements)
      val (firstHalf, secondHalf) = keyValues.splitAt(half)

      insertKeyValuesToPartition(firstHalf, partitionNumber)
      val identityFlow = Flow[ConsumerRecord[KeyValue]]
      val toResult: List[ConsumerRecord[KeyValue]] => List[(Int, String, String)] =
        _.map(r => (r.partitionNumber, readKey(r), readValue(r))).sorted
      val obtainedResult1 = toResult(runConsumerAndCollectData(half, identityFlow))
      insertKeyValuesToPartition(secondHalf, partitionNumber)
      val obtainedResult2 = toResult(runConsumerAndCollectData(half, identityFlow))

      val toExpectedResult: List[(String, String)] => List[(Int, String, String)] =
        _.map { case (k, v) => (partitionNumber, k, v) }
      val expectedResult = toExpectedResult(firstHalf)
      assert(obtainedResult1 === expectedResult)
      assert(obtainedResult2 === expectedResult)
    }
  }

  it should "stop streaming from partition if newer lock already committed its value" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 10
      val keyValues = createKeyValues(numberOfElements)
      val partitionNumber = 0
      insertKeyValuesToPartition(keyValues, partitionNumber)
      val consumer = createConsumer()
      val result =
        consumer
          .throttle(1, 100.millis)
          .zipWithIndex
          .mapAsync(1) {
            case (r, i) =>
              val future =
                // update `acquiredWith` field with newer versionstamp in the middle of processing
                if (i == (numberOfElements / 2))
                  updateConsumerGroupMetadataUpdatedWithLock(partitionNumber)
                else Future.successful(())
              future.map(_ => r)
          }
          .via(Consumer.committableFlow(database))
          .mergeSubstreams
          // collect items before refreshing locks fires up
          .takeWithin(1.second)
          .runWith(Sink.seq)
          .await
          .toList
      val obtainedData = result.map(r => (r.partitionNumber, readKey(r), readValue(r)))
      val expectedData =
        keyValues.take(numberOfElements / 2).map { case (k, v) => (partitionNumber, k, v) }
      assert(obtainedData === expectedData)
    }
  }

  it should "stop streaming from partition if newer lock already committed its value when " +
    "commit appeared after flattening streams" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 10
      val keyValues = createKeyValues(numberOfElements)
      val partitionNumber = 0
      insertKeyValuesToPartition(keyValues, partitionNumber)
      val consumer = createConsumer()
      val result =
        consumer
          .throttle(1, 100.millis)
          .mergeSubstreams
          .zipWithIndex
          .mapAsync(1) {
            case (r, i) =>
              val future =
                // update `acquiredWith` field with newer versionstamp in the middle of processing
                if (i == (numberOfElements / 2))
                  updateConsumerGroupMetadataUpdatedWithLock(partitionNumber)
                else Future.successful(())
              future.map(_ => r)
          }
          .via(Consumer.committableFlow(database))
          // collect items before refreshing locks fires up
          .takeWithin(1.second)
          .runWith(Sink.seq)
          .await
          .toList
      val obtainedData = result.map(r => (r.partitionNumber, readKey(r), readValue(r)))
      val expectedData =
        keyValues.take(numberOfElements / 2).map { case (k, v) => (partitionNumber, k, v) }
      assert(obtainedData === expectedData)
    }
  }

  it should "refresh its locks and underlying transaction (streaming over 5 seconds)" in new TopicMetadataFixture {
    withMaterializer { implicit mat =>
      val numberOfElements = 10
      val keyValues = createKeyValues(numberOfElements)
      val partitionNumber = 0
      insertKeyValuesToPartition(keyValues, partitionNumber)
      val consumer = createConsumer()

      val result =
        consumer.mergeSubstreams
          .throttle(1, 1.second)
          .via(Consumer.committableFlow(database))
          .take(numberOfElements.toLong)
          .runWith(Sink.seq)
          .await(11.seconds)
          .toList
      val obtainedData = result.map(r => (r.partitionNumber, readKey(r), readValue(r)))
      val expectedData = keyValues.map { case (k, v) => (partitionNumber, k, v) }
      assert(obtainedData === expectedData)
    }
  }

  private def updateConsumerGroupMetadataUpdatedWithLock(partitionNumber: Int): Future[Unit] = {
    val futureVersionstamp =
      typedLockSubspace
        .set(
          ConsumerLock(
            key = ConsumerLockKey("randomTopic", "random", 0),
            acquiredWith = Versionstamp.incomplete,
            refreshedAt = Instant.now,
            acquiredBy = UUID.randomUUID))
        .transactVersionstamped(database)
        .collect { case (_, Some(v)) => v }
    val key =
      ConsumerGroupMetadataKey(
        topic = topic,
        consumerGroup = consumerGroup,
        partitionNo = partitionNumber)
    def updateVersionstamp(newV: Versionstamp): DBIO[Unit] =
      for {
        metadata <- typedConsumerGroupMetadataSubspace.get(key): DBIO[Option[ConsumerGroupMetadata]]
        _ <- typedConsumerGroupMetadataSubspace.set(metadata.get.copy(updatedWithLock = newV))
      } yield ()
    for {
      v <- futureVersionstamp
      _ <- updateVersionstamp(v).transact(database)
    } yield ()
  }

  private def runConsumerAndCollectData(
      numberOfElements: Int,
      flow: Flow[ConsumerRecord[KeyValue], ConsumerRecord[KeyValue], NotUsed])(
      implicit mat: ActorMaterializer): List[ConsumerRecord[KeyValue]] = {
    createConsumer()
      .via(flow)
      .mergeSubstreams
      .take(numberOfElements.toLong)
      .runWith(Sink.seq)
      .await
      .toList
  }

  private def createConsumer()(implicit mat: ActorMaterializer): PubSubClient.ConsumerSubSource = {
    pubSubClient.consumer(topic, consumerGroup, ConsumerSettings())
  }

  private def insertKeyValuesToPartition(
      keyValues: Seq[(String, String)],
      partitionNumber: Int): Unit = {
    keyValues.foreach {
      case (k, v) =>
        producer
          .send(topic, partitionNumber, Tuple.from(k).pack, Tuple.from(v).pack)
          .transact(database)
          .await
    }
  }

  private def insertKeyValues(keyValues: Seq[(String, String)]): Unit = {
    keyValues.foreach {
      case (k, v) =>
        producer
          .send(topic, Tuple.from(k).pack, Tuple.from(v).pack)
          .transact(database)
          .await
    }
  }
}
