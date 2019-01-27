package com.github.pwliwanow.fdb.pubsub.example;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.SubSource;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.github.pwliwanow.fdb.pubsub.ConsumerRecord;
import com.github.pwliwanow.fdb.pubsub.ConsumerSettings;
import com.github.pwliwanow.fdb.pubsub.javadsl.Consumer;
import com.github.pwliwanow.fdb.pubsub.javadsl.Producer;
import com.github.pwliwanow.fdb.pubsub.javadsl.PubSubClient;
import scala.concurrent.ExecutionContextExecutor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

public class ExampleJava {

    private static int NUMBER_OF_PARTITIONS = 10;
    private static int NUMBER_OF_ELEMENTS_TO_PRODUCE = 1000;

    private static String TOPIC = "exampleTopic";
    private static String CONSUMER_GROUP = "exampleConsumerGroup";

    private static boolean SHOW_CONSUMED_ELEMENTS = false;

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create();
        Database db = FDB.selectAPIVersion(600).open(null, system.dispatcher());

        Subspace pubSubSubspace = new Subspace(Tuple.from("PubSubExample"));
        db.run(tx -> {
            tx.clear(pubSubSubspace.range());
            return NotUsed.getInstance();
        });

        PubSubClient pubSubClient = PubSubClient.create(pubSubSubspace, db);
        pubSubClient.createTopic(TOPIC, NUMBER_OF_PARTITIONS, system.dispatcher()).toCompletableFuture().join();

        produceElements(pubSubClient.producer(), db);

        consumeElements(pubSubClient, db, system);

        system.terminate();
        system.getWhenTerminated().thenAccept(x -> db.close());
    }

    private static void produceElements(Producer producer, Database database) {
        long timeInMs = measure(() -> doProduce(producer, database));
        System.out.println("Producing " + NUMBER_OF_ELEMENTS_TO_PRODUCE + " elements took " + timeInMs + " ms.");
    }

    private static void doProduce(Producer producer, Database database) {
        // note that elements are not produced in order (at least there is no such guarantee)
        CompletableFuture[] futures = IntStream
                .range(0, NUMBER_OF_ELEMENTS_TO_PRODUCE)
                .mapToObj(i ->
                        producer.send(
                                database,
                                TOPIC,
                                Tuple.from(String.format("ExampleKey%07d", i)).pack(),
                                Tuple.from("ExampleValue" + i).pack())
                ).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
    }

    private static void consumeElements(PubSubClient pubSubClient, Database database, ActorSystem system) {
        long timeInMs = measure(() -> doConsumeElements(pubSubClient, database, system));
        System.out.println(
                "Consuming " + NUMBER_OF_ELEMENTS_TO_PRODUCE + " elements took " +
                timeInMs + " ms.");
    }

    private static void doConsumeElements(PubSubClient pubSubClient, Database database, ActorSystem system) {
        Materializer mat = ActorMaterializer.create(system);
        ExecutionContextExecutor ec = mat.executionContext();
        SubSource<ConsumerRecord<KeyValue>, NotUsed> consumer =
                pubSubClient.consumer(TOPIC, CONSUMER_GROUP, ConsumerSettings.create(), mat);
        List<PartitionWithKV> consumedElements = consumer
                .map(record ->
                        record.map(kv -> {
                            String key = Tuple.fromBytes(kv.getKey()).getString(0);
                            String value = Tuple.fromBytes(kv.getValue()).getString(0);
                            return new KV(key, value);
                        })
                )
                .via(Consumer.committableFlow(database, ec))
                .mergeSubstreams()
                // limit number of elements to terminate the consumer
                .take(NUMBER_OF_ELEMENTS_TO_PRODUCE)
                .map(record ->
                        new PartitionWithKV(
                                record.partitionNumber(),
                                record.data().getKey(),
                                record.data().getValue()))
                .runWith(Sink.seq(), mat)
                .toCompletableFuture()
                .join();

        if (SHOW_CONSUMED_ELEMENTS) {
            consumedElements.forEach(x -> System.out.println(x.toString()));
        }
    }

    private static long measure(Runnable f) {
        long before = System.nanoTime();
        f.run();
        long after = System.nanoTime();
        return (long) ((after - before) / 10e5);
    }

    private static class KV {
        private final String key;
        private final String value;

        KV(String key, String value) {
            this.key = key;
            this.value = value;
        }

        String getKey() {
            return key;
        }

        String getValue() {
            return value;
        }
    }

    private static class PartitionWithKV {
        private final int partitionNumber;
        private final String key;
        private final String value;

        PartitionWithKV(int partitionNumber, String key, String value) {
            this.partitionNumber = partitionNumber;
            this.key = key;
            this.value = value;
        }

        public String toString() {
            return "(" + partitionNumber + ", " + key + ", " + value +")";
        }
    }

}
