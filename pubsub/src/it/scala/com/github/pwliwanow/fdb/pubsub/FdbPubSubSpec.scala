package com.github.pwliwanow.fdb.pubsub

import java.time.{Clock, ZoneOffset}
import java.util

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.apple.foundationdb.{Database, FDB, KeyValue}
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.fdb.pubsub.internal.common.{TopicMetadataSubspace, TopicRecord, TopicSubspace}
import com.github.pwliwanow.fdb.pubsub.internal.locking.ConsumersLockSubspace
import com.github.pwliwanow.fdb.pubsub.internal.metadata.{ConsumerGroupMetadataSubspace, TopicMetadataService}
import com.github.pwliwanow.fdb.pubsub.scaladsl.PubSubClient
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

abstract class FdbPubSubSpec
    extends TestKit(ActorSystem())
    with FlatSpecLike
    with BeforeAndAfterEach {

  protected val database: Database = FDB.selectAPIVersion(600).open(null, system.dispatcher)

  implicit def ec: ExecutionContextExecutor = system.dispatcher

  protected val topic = "testTopic"
  protected val numberOfPartitions = 10

  protected val utcClock = Clock.tickMillis(ZoneOffset.UTC)

  protected val testSubspace = new Subspace(Tuple.from("test"))

  protected val topicSubspace = new Subspace(testSubspace.pack(Tuple.from("Topics")))
  protected val typedTopicSubspace = new TopicSubspace(topicSubspace)
  protected val topicMetadataSubspace = new Subspace(testSubspace.pack(Tuple.from("TopicsMeta")))
  protected val typedTopicMetadataSubspace = new TopicMetadataSubspace(topicMetadataSubspace)

  protected val consumerGroupMetadataSubspace = new Subspace(
    testSubspace.pack(Tuple.from("GroupsMeta")))
  protected val typedConsumerGroupMetadataSubspace =
    new ConsumerGroupMetadataSubspace(consumerGroupMetadataSubspace)
  protected val lockSubspace = new Subspace(testSubspace.pack(Tuple.from("Locks")))
  protected val typedLockSubspace = new ConsumersLockSubspace(lockSubspace)

  protected val topicMetadataService = new TopicMetadataService(typedTopicMetadataSubspace)

  protected val pubSubClient = PubSubClient(testSubspace, database)

  protected val producer = pubSubClient.producer

  implicit class FutureHolder[A](value: Future[A]) {
    def await: A = await(5.seconds)
    def await(duration: Duration): A = Await.result(value, duration)
  }

  override def afterEach(): Unit = {
    database.run(_.clear(testSubspace.range()))
  }

  trait TopicMetadataFixture {
    topicMetadataService.createTopic(topic, numberOfPartitions).transact(database).await
  }

  def withMaterializer[A](f: ActorMaterializer => A): A = {
    val mat = ActorMaterializer()
    val res = Try(f(mat))
    mat.shutdown()
    res.get
  }

  protected def createKeyValues(n: Int): List[(String, String)] = {
    (1 to n).map(i => f"TestKey$i%04d" -> s"TestValue$i").toList
  }

  protected def readKey(record: TopicRecord): String = {
    Tuple.fromBytes(record.userData.getKey).getString(0)
  }

  protected def readKey(record: ConsumerRecord[KeyValue]): String = {
    Tuple.fromBytes(record.data.getKey).getString(0)
  }

  protected def readValue(record: TopicRecord): String = {
    Tuple.fromBytes(record.userData.getValue).getString(0)
  }

  protected def readValue(record: ConsumerRecord[KeyValue]): String = {
    Tuple.fromBytes(record.data.getValue).getString(0)
  }

  protected def expectedPartition(byteArray: Array[Byte]): Int = {
    Math.abs(util.Arrays.hashCode(byteArray)) % numberOfPartitions
  }

  protected def expectedPartition(key: String): Int = {
    Math.abs(util.Arrays.hashCode(Tuple.from(key).pack)) % numberOfPartitions
  }
}
