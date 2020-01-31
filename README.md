# FDB-PubSub

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.pwliwanow.fdb-pubsub/pubsub_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.pwliwanow.fdb-pubsub/pubsub_2.12)
[![Build Status](https://travis-ci.org/pwliwanow/fdb-pubsub.svg?branch=master)](https://travis-ci.org/pwliwanow/fdb-pubsub)
[![codecov](https://codecov.io/gh/pwliwanow/fdb-pubsub/branch/master/graph/badge.svg)](https://codecov.io/gh/pwliwanow/fdb-pubsub)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-brightgreen.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

FDB-PubSub is a publish subscribe layer for [FoundationDB](https://apple.github.io/foundationdb/index.html), built on top of [Akka Streams](https://doc.akka.io/docs/akka/2.5/stream/stream-introduction.html) and it provides Java and Scala API. It is inspired by Kafka.

### Motivation
Getting data from the database to publish subscribe system is [surprisingly hard](https://www.confluent.io/blog/bottled-water-real-time-integration-of-postgresql-and-kafka/). 
It would be much simpler if developer could publish the event within the business transaction - and it's exactly what FDB-PubSub does, it supports publishing events within FoundationDB transaction.

### Features:
- Support for publishing events and committing offsets within FoundationDB transaction
- Easily scalable and fault tolerant storage (thanks to FoundationDB)
- Easy integration with Apache Cassandra, Apache Kafka, Elasticsearch and more (thanks to [Akka Streams](https://doc.akka.io/docs/akka/2.5/stream/stream-introduction.html) and [Alpakka](https://doc.akka.io/docs/alpakka/current/))
- Exposed as a library, so if you already opearate FoundationDB there is no new stateful component to maintain

### Overview
Concepts and assumptions are similar to Kafka. Here is brief overview of FDB-PubSub:
- User data can be published to _topics_
- Topics are divided into _partitions_
- Number of partitions is defined during topic creation
- User data consists of key (byte array) and value (byte array)
- Values with the same key will end up in the same partition (unless user specified different partition explicitly)
- Records are _ordered_ within the partition
- Within topic each record has unique _offset_ assigned. Offset is of type [Versionstamp](https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/tuple/Versionstamp.html)
- Each consumer belongs to a _consumer group_
- Records within a partition will be processed in order
- Records for consumers with the same consumer groups will be load balanced (e.g. given topic with 10 partitions if at first there is a single consumer `A` that process data from all partitions, and consumer `B` joins, consumer `B` will take over processing data from 5 partitions and consumer `A` will continue processing data from the other 5 partitions)
- Consumers keep track of its position by storing last processed offset from each partition (i.e. offset is being stored for every tuple `(topic, consumerGroup, partitionNumber)`)

### Current limitations
- It's not yet production ready. If you'd like to use it in your project, please get it touch, I will be happy to help.
- To stream data from a partition consumers acquire locks (those locks are later used to atomically commit offset; outdated locks fail to commit an offset and underlying partition stream is stopped). Currently, those locks are being acquired rather aggressively by consumers that joined: consumer that held a lock is not aware about the fact that other consumer wants to join, and newly connected consumer simply acquires some of the locks that were held by others. 
As the result, when processing data using at least once delivery semantics, it causes messages to be processed more times that it would be necessary if locks were acquired gracefully. It will be addressed in future releases.
- No performance tests were performed as of now. Currently - with default settings - having up to 10 consumers and 1000 partitions per topic should be perfectly fine.

### Disclaimer
It's not well-tested on production. I'd recommend using Kafka or Pulsar instead. 

# Quickstart
FDB-PubSub provides both Java and Scala API. Java API is present in package `com.github.pwliwanow.fdb.pubsub.javadsl` and Scala API is present in `com.github.pwliwanow.fdb.pubsub.scaladsl`. Module `example` contains small examples written in Java and in Scala.

### Dependency
To get started with FDB-PubSub add the following dependency with SBT:
```scala
val fdbPubSubVersion = "0.2.0"
libraryDependencies += "com.github.pwliwanow.fdb-pubsub" %% "pubsub" % fdbPubSubVersion
```
or Maven:
```xml
<dependency>
  <groupId>com.github.pwliwanow.fdb-pubsub</groupId>
  <artifactId>pubsub_2.12</artifactId>
  <version>0.2.0</version>
</dependency>
```

### PubSubClient
To start you need to create a `PubSubClient`, it's an immutable class that can be freely shared within the application. It allows user to create a topic, get producer and create a consumer. `PubSubClient` requires a `Subspace` that it will operate on and a `Database` to be provided:
```scala
// Scala
import com.github.pwliwanow.fdb.pubsub.scaladsl.PubSubClient

val system = ActorSystem()
implicit ec = system.dispacher()
val db = FDB.selectAPIVersion(620).open(null, ec)
val pubSubSubspace = new Subspace(Tuple.from("PubSubExample"))
val pubSubClient = PubSubClient(pubSubSubspace, database)
```

```java
// Java
import com.github.pwliwanow.fdb.pubsub.javadsl.PubSubClient;

ActorSystem system = ActorSystem.create();
ExecutionContextExecutor ec = system.dispatcher();
Database db = FDB.selectAPIVersion(600).open(null, ec);
Subspace pubSubSubspace = new Subspace(Tuple.from("PubSubExample"));
PubSubClient pubSubClient = PubSubClient.create(pubSubSubspace, db);
```

### Creating a topic
```scala
// Scala
val futureResult: Future[NotUsed] = pubSubClient.createTopic("testTopic", numberOfPartitions = 10)
```
```java
// Java
int numberOfPartitions = 10;
CompletableFuture<NotUsed> futureResult = pubSubClient.createTopic("testTopic", numberOfPartitions, ec);
```

### Using a producer
Producers from Java and Scala API differ in how they compose transactions. Java API takes additional `TransactionContext` as a parameter and returns `CompletableFuture<NotUsed>`, and Scala API returns `DBIO[NotUsed]` (which is a type from [foundationdb4s](https://github.com/pwliwanow/foundationdb4s)).

```scala
// Scala
val producer = pubSubClient.producer
val dbio = 
  producer.send(
    "testTopic", 
    Tuple.from("ExampleKey").pack(), 
    Tuple.from("ExampleValue").pack())
val futureResult: Future[NotUsed] = dbio.transact(database);
```
```java
// Java
Producer producer = pubSubClient.producer();
CompletableFuture<NotUsed> futureResult = 
  database.runAsync(tx -> 
    producer.send(
      tx, 
      "testTopic", 
      Tuple.from("ExampleKey").pack(), 
      Tuple.from("ExampleValue").pack()));
```

### Creating a consumer
Consumers are exposed as [substreams](https://doc.akka.io/docs/akka/2.5/stream/stream-substream.html), where each partition forms a separate stream 
(which is especially useful during committing offsets, when each stream may perform commit action independently).
To create a consumer _topic_, _consumerGroup_, _ConsumerSettings_ and _Materializer_ need to be provided:
```scala
// Scala
implicit val mat = ActorMaterializer()
val defaultSettings = ConsumerSettings()
val consumer = pubSubClient.consumer("testTopic", "testConsumerGroup", defaultSettings)
```
```java
// Java
Materializer mat = ActorMaterializer.create();
ConsumerSettings defaultSettings = ConsumerSettings.create();
SubSource<ConsumerRecord<KeyValue>, NotUsed> consumer = 
  pubSubClient.consumer("testTopic", "testConsumerGroup", defaultSettings, mat);
```

### Committing offsets
FDB-PubSub offers `committableFlow` and `committableSink` that should be used for committing offsets. It's guaranteed to be run exactly once for each `ConsumerRecord`. 
Optionally, user can add custom transactional logic.
```scala
// Scala
def transactionToCompose(record: ConsumerRecord[KeyValue]): DBIO[Unit] = {
  // some implementation
}
val runnableGraph = consumer.to(Consumer.committableSink(database, transactionToCompose))
```
```java
// Java
RunnableGraph<NotUsed> runnableGraph =
  consumer.to(Consumer.committableSink(database, (tx, record) -> performTransaction(tx, record), ec));

CompletableFuture<Void> performTransaction(TransactionContext tx, ConsumerRecord<KeyValue> record) {
  // some implementation
}
```

### Running the consumer
```scala
// Scala
runnableGraph.run()
```
```java
// Java
runnableGraph.run(mat);
```

## Semantics
Depending on the use case, different processing semantics may become useful

### Exactly once
Exactly once processing is only possible within FoundationDB by using `committableFlow` or `committableSink`, as shown in _Committing offsets_ section.

### At least once
To process data at least once, additional processing should be done before offset is committed. 
Depending on your use case it may be a good idea to commit offsets in batch:
```scala
// Scala
def updateElasticsearchInBatch(records: Seq[ConsumerRecord[Entity]]): Future[Unit] = {
  // implementation here
}

consumer
  .groupedWithin(1000, 5.seconds)
  // at the end get only the last record to perform batch commit
  .mapAsync(1)(records => updateElasticsearchInBatch(records).map(_ => records.last))
  .via(Consumer.commitableFlow(database))
```
```java
// Java
consumer
  .groupedWithin(1000, Duration.of(5, ChronoUnit.SECONDS))
  // at the end get only the last record to perform batch commit
  .mapAsync(1, records -> updateElasticsearchInBatch(records).thenApply(() -> records.get(records.size() - 1)))
  .via(Consumer.commitableFlow(database));

CompletionStage<Void> updateElasticsearchInBatch(List<ConsumerRecord<Entity>> records) {
  // implementation here
}
```

### At most once
To process data at most once, addtional processing should be done after offset is committed:
```scala
// Scala
def sendNotCriticalNotification(record: ConsumerRecord[Entity]): Future[Unit] = {
  // implementation here
}
val parallelism = 10
consumer
  .via(Consumer.commitableFlow(database))
  .mapAsync(parallelism)(sendNotCriticalNotification)
  .to(Sink.ignore)
  .run()
```
```java
// Java
int parallelism = 10;
consumer
  .via(Consumer.commitableFlow(database))
  .mapAsync(parallelism, this::sendNotCriticalNotification)
  .to(Sink.ignore)
  .run(mat);

CompletionStage<Void> sendNotCriticalNotification(ConsumerRecord<Entity> record) {
  // implementation here
}
```

### Cleaning data from topics
To enable cleaning data from topics three basic methods are provided: `clear`, `getOffset` and `getPartitions`.
Those methods can be combined as follows:
```scala
// Scala
import cats.implicits._
val topic = "products"
val consumerGroups = List("cg1", "cg2")
val dbio = for {
  partitions <- pubSubClient.getPartitions(topic).toDBIO
  consumerGroupPartitions = partitions.flatMap(p => consumerGroups.map(c => (p, c)))
  _ <- consumerGroupPartitions.parTraverse { case (p, c) =>
    pubSubClient
      .getOffset(topic, c, p)
      .toDBIO
      .flatMap(_.fold(DBIO.unit)(offset => pubSubClient.clear(topic, p, offset)))
  }
} yield ()
dbio.transact(database)
```
```java
// Java, for simplicity example below is blocking
String topic = "products";
List<String> consumerGroups = Arrays.asList("cg1", "cg2");
db.run((Transaction tr) -> {
    List<Int> partitions = pubSubClient.getPartitions(tr, topic).join();
    for (String c: consumerGroups)
        for (int p: partitions) {
            pubSubClient.getOffset(tr, topic, p, c).join().ifPresent(offset -> {
                pubSubClient.clear(tr, topic, p, offset).join();
            });
        }
});
```
