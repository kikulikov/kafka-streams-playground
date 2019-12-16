package io.confluent.playground;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
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

import java.util.Properties;
import java.util.UUID;

@Deprecated
public class CompoundTimestampTest {

    private static final String ClicksStoreName = "clicks-store";

    // As a data analyst,
    // I want to get all events which occurred at a particular time range
    // so that I could analyse them in batches (buckets).

    static class TimestampProcessor<K, V> implements Processor<K, V> {

        // TODO State store name as a constructor parameter
        private KeyValueStore<byte[], V> stateStore;

        @Override
        public void init(ProcessorContext context) {
            // TODO what can be done about class cast here
            this.stateStore = (KeyValueStore<byte[], V>) context.getStateStore(ClicksStoreName);
        }

        // TODO time
        // See RocksDBWindowStore, WrappedStateStore, WindowKeySchema
        //
        // Event-time
        // Ingestion-time
        // Processing-time
        //
        // Interactive queries
        //
        // How is it different from WindowStore?

        @Override
        public void process(K key, V value) {
            // TODO Pair.of() -> Serialized
            System.out.println("Process >>> >>> " + key + ", " + value);

            // TODO where to take timestamp?
//            stateStore.put(ByteMethods.BytesKey.toBinaryKey(key.toString(), value.toString()).get(),
//                    key.toString() + value.toString());
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testTimestampProcessor() {

        System.out.println();

        // Click [User ID, Page Link]
        final Serde<UUID> uuidSerde = Serdes.UUID();
        final Serde<String> stringSerde = Serdes.String();

        // ==============
        // Build topology
        // ==============
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<UUID, String> clicks = builder.stream("clicks", Consumed.with(uuidSerde, stringSerde));

        // debug print
        clicks.peek((key, value) -> System.out.println("Click >>> " + key + ", " + value));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ClicksStoreName), uuidSerde, stringSerde));

        // TODO Why state store name is everywhere?
        clicks.process(() -> new TimestampProcessor<UUID, String>(), ClicksStoreName);

        // TODO Why not windowing?
        // clicks.groupByKey().reduce((s1, s2) -> s1 + s2)
        // .toStream().peek((key, value) -> System.out.println("Grouped: " + key + ", " + value));

        final Topology topology = builder.build();
//        System.out.println(topology.describe());

        // =============
        // Test topology
        // =============
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

        final ConsumerRecordFactory<UUID, String> clicksFactory =
                new ConsumerRecordFactory<>("clicks", new UUIDSerializer(), new StringSerializer());

        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();

        testDriver.pipeInput(clicksFactory.create(uuid1, "https://google.com"));
        testDriver.pipeInput(clicksFactory.create(uuid2, "https://www.reddit.com"));
        testDriver.pipeInput(clicksFactory.create(uuid1, "https://confluent.io"));
        testDriver.pipeInput(clicksFactory.create(uuid1, "https://confluent.io/blog"));
        testDriver.pipeInput(clicksFactory.create(uuid2, "https://www.reddit.com/r/apachekafka/"));

        testDriver.getAllStateStores().forEach((s, stateStore) -> System.out.println(s + ", " + stateStore));

//        ReadOnlyKeyValueStore<String, Long> keyValueStore =
//                streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<String, Long> keyValueStore = testDriver.getKeyValueStore(ClicksStoreName);

        // keyValueStore.range()

        // keyValueStore.get(BytesKey.toBinaryKey(uuid2.toString(), "8"));
        // TODO get(String) vs get(Bytes)

//        System.out.println(testDriver.readOutput("result-topic", new UUIDDeserializer(), new LongDeserializer()));
//        System.out.println(testDriver.readOutput("result-topic", new UUIDDeserializer(), new LongDeserializer()));
//        System.out.println(testDriver.readOutput("result-topic", new UUIDDeserializer(), new LongDeserializer()));

        // TODO KeyValueStore store = testDriver.getKeyValueStore("store-name");

        // OutputVerifier.compareKeyValue(outputRecord, "key", 42L);
    }
}
