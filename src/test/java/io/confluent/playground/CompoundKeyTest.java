package io.confluent.playground;

import kafka.streams.compound.ByteMethods;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;
import kafka.streams.compound.ByteMethods.*;

@Deprecated
public class CompoundKeyTest {

    // TODO https://github.com/confluentinc/kafka-streams-examples/blob/5.3.1-post/src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java

    private static final String ORDERS_STORE_NAME = "orders-state-store";

    static class ClicksProcessor<K, V> implements Processor<K, V> {

        private ProcessorContext context;
        private KeyValueStore<byte[], String> ordersStore;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.ordersStore = (KeyValueStore<byte[], String>) context.getStateStore(ORDERS_STORE_NAME);
        }

        @Override
        public void process(K key, V value) {
            byte[] compoundKey = ByteKey.toBinaryKey(key.hashCode(), value.hashCode()).get();
            // TODO compound value as Pair.of() -> Serialized
            System.out.println(Arrays.toString(compoundKey));
            ordersStore.put(compoundKey, key.toString() + "+" + value.toString());

            context.commit();
        }

        @Override
        public void close() {
            ordersStore.close();
        }
    }

    private Topology buildCompoundTopology() {

        final Serde<byte[]> byteSerde = Serdes.ByteArray();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> clicksTable = builder.stream("clicks", Consumed.with(longSerde, stringSerde));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ORDERS_STORE_NAME), byteSerde, stringSerde));

        final ClicksProcessor<Long, String> processor = new ClicksProcessor<>();

        clicksTable.peek((key, value) -> System.out.println("Click >>> " + key + ", " + value))
                .process(() -> processor, ORDERS_STORE_NAME);

        return builder.build();
    }

    @Test
    public void testCompoundTopology() {

        final Topology topology = buildCompoundTopology();
        System.out.println("\n" + topology.describe());

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

        final ConsumerRecordFactory<Long, String> clicksFactory =
                new ConsumerRecordFactory<>("clicks", new LongSerializer(), new StringSerializer());

        testDriver.pipeInput(clicksFactory.create(101L, "https://www.reddit.com"));
        testDriver.pipeInput(clicksFactory.create(102L, "https://confluent.io"));
        testDriver.pipeInput(clicksFactory.create(102L, "https://confluent.io/blog"));
        testDriver.pipeInput(clicksFactory.create(101L, "https://www.reddit.com/r/apachekafka/"));
        testDriver.pipeInput(clicksFactory.create(102L, "https://www.confluent.io/resources"));
        testDriver.pipeInput(clicksFactory.create(103L, "https://confluent.io"));
        testDriver.pipeInput(clicksFactory.create(103L, "https://confluent.io/blog"));

        testDriver.getAllStateStores().forEach((s, stateStore) -> System.out.println(s + ", " + stateStore));

        ReadOnlyKeyValueStore<byte[], String> keyValueStore = testDriver.getKeyValueStore(ORDERS_STORE_NAME);

        System.out.println(Arrays.toString(ByteMethods.lowerRangeHigherByte("103").get()));
        System.out.println(Arrays.toString(ByteMethods.upperRangeHigherByte("103").get()));

        keyValueStore.range(ByteMethods.lowerRangeHigherByte("103").get(),
                ByteMethods.upperRangeHigherByte("103").get())
                .forEachRemaining(System.out::println);

//        ReadOnlyKeyValueStore<byte[], String> keyValueStore = testDriver.getKeyValueStore(ORDERS_STORE_NAME);

        System.out.println(Arrays.toString(ByteMethods.lowerRangeLowerByte("https://confluent.io/blog").get()));
        System.out.println(Arrays.toString(ByteMethods.upperRangeLowerByte("https://confluent.io/blog").get()));

        keyValueStore.range(ByteMethods.lowerRangeLowerByte("https://confluent.io/blog").get(),
                ByteMethods.upperRangeLowerByte("https://confluent.io/blog").get())
                .forEachRemaining(System.out::println);

        // TODO think about results (everything)

//        keyValueStore.range(ByteMethods.ByteEncoder.toBinaryKeyForLowerRange("103").get(),
//                ByteMethods.ByteEncoder.toBinaryKeyForUpperRange("103").get())
//                .forEachRemaining(stringKeyValue -> System.out.println(stringKeyValue));
    }

//    public static class Unit<A> {
//
//        public final A a;
//
//        public Unit(A a) {
//            this.a = a;
//        }
//
//        public static <AA> AppTest.Unit<AA> of(AA aa) {
//            return new AppTest.Unit<>(aa);
//        }
//    }
//
//    public static class Pair<A, B> {
//
//        public final A a;
//        public final B b;
//
//        public Pair(A a, B b) {
//            this.a = a;
//            this.b = b;
//        }
//
//        public static <AA, BB> AppTest.Pair<AA, BB> of(AA aa, BB bb) {
//            return new AppTest.Pair<>(aa, bb);
//        }
//    }
//
//    public static class Triplet<A, B, C> {
//
//        public final A a;
//        public final B b;
//        public final C c;
//
//        public Triplet(A a, B b, C c) {
//            this.a = a;
//            this.b = b;
//            this.c = c;
//        }
//
//        public static <AA, BB, CC> AppTest.Triplet<AA, BB, CC> of(AA aa, BB bb, CC cc) {
//            return new AppTest.Triplet<>(aa, bb, cc);
//        }
//    }
}
