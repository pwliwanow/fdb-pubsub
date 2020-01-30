package com.github.pwliwanow.fdb.pubsub.example
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{Database, FDB}
import com.github.pwliwanow.fdb.pubsub.ConsumerSettings
import com.github.pwliwanow.fdb.pubsub.scaladsl.{Consumer, Producer, PubSubClient}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object ExampleScala {

  val numberOfPartitions = 10
  val numberOfElementsToProduce = 1000

  val topic = "exampleTopic"
  val consumerGroup = "exampleConsumerGroup"

  val showConsumedElements = false

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit def ec: ExecutionContextExecutor = system.dispatcher
    val database: Database = FDB.selectAPIVersion(600).open(null, ec)

    val pubSubSubspace = new Subspace(Tuple.from("PubSubExample"))
    database.run(tx => tx.clear(pubSubSubspace.range))

    val pubSubClient = PubSubClient(pubSubSubspace, database)
    pubSubClient.createTopic(topic, numberOfPartitions).await

    produceElements(pubSubClient.producer, database)

    consumeElements(pubSubClient, database)

    system.terminate().onComplete { _ =>
      database.close()
    }
  }

  def produceElements(producer: Producer, database: Database)(
      implicit ec: ExecutionContextExecutor): Unit = {
    // note that elements are not produced in order (at least there is no such guarantee)
    val (_, producingElementsTimeMs) = measure {
      val listOfFutures =
        (1 to numberOfElementsToProduce)
          .map { i =>
            producer
              .send(
                topic,
                Tuple.from(f"ExampleKey$i%07d").pack(),
                Tuple.from(s"ExampleValue$i").pack())
              .transact(database)
          }
      Future
        .sequence(listOfFutures)
        .await
    }

    println(s"Producing $numberOfElementsToProduce elements took $producingElementsTimeMs ms.")
  }

  def consumeElements(pubSubClient: PubSubClient, database: Database)(
      implicit ec: ExecutionContextExecutor,
      system: ActorSystem): Unit = {
    val consumer = pubSubClient.consumer(topic, consumerGroup, ConsumerSettings())
    val (consumedElements, consumingElementsTimeMs) = measure {
      consumer
        .map { record =>
          record.map { kv =>
            val key = Tuple.fromBytes(kv.getKey).getString(0)
            val value = Tuple.fromBytes(kv.getValue).getString(0)
            (key, value)
          }
        }
        .via(Consumer.committableFlow(database))
        .mergeSubstreams
        // limit number of elements to terminate the consumer
        .take(numberOfElementsToProduce.toLong)
        .map(record => (record.partitionNumber, record.data._1, record.data._2))
        .runWith(Sink.seq)
        .await
    }
    if (showConsumedElements) consumedElements.foreach(println)
    println(s"Consuming $numberOfElementsToProduce elements took $consumingElementsTimeMs ms.")
  }

  implicit class FutureHolder[A](val future: Future[A]) extends AnyVal {
    def await: A = Await.result(future, Duration.Inf)
  }

  def measure[A](f: => A): (A, Long) = {
    val before = System.nanoTime()
    val result = f
    val after = System.nanoTime()
    (result, ((after - before) / 10e5).toLong)
  }

}
