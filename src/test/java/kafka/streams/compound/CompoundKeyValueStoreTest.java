package kafka.streams.compound;

import kafka.streams.compound.JsonSerializerDeserializer.JsonDeserializer;
import kafka.streams.compound.JsonSerializerDeserializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompoundKeyValueStoreTest {

    @Test
    void range() {
        final String stateStoreName = "test-state-store";
        final StreamsBuilder builder = new StreamsBuilder();

        final JsonSerializer<KeyValue<Integer, String>> pairSer = new JsonSerializer<>();

        @SuppressWarnings("unchecked") final JsonDeserializer<KeyValue<Integer, String>> pairDes =
                new JsonDeserializer<>((Class<KeyValue<Integer, String>>) (Object) KeyValue.class);

        final Serde<Bytes> byteSerde = Serdes.Bytes();
        final Serde<Integer> intSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<KeyValue<Integer, String>> pairSerde = Serdes.serdeFrom(pairSer, pairDes);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(stateStoreName), byteSerde, pairSerde));

        final CompoundKeyValueProcessor<Integer, String, String> proc =
                new CompoundKeyValueProcessor<>(stateStoreName, s -> s);

        builder.stream("test-topic", Consumed.with(intSerde, stringSerde)).process(() -> proc, stateStoreName);

        final Topology topology = builder.build();

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

        proc.process(101, "https://www.reddit.com");
        proc.process(102, "https://confluent.io");
        proc.process(102, "https://confluent.io/blog");
        proc.process(101, "https://www.reddit.com/r/apachekafka/");
        proc.process(102, "https://www.confluent.io/resources");
        proc.process(103, "https://confluent.io");
        proc.process(103, "https://confluent.io/blog");

        testDriver.getAllStateStores().forEach((s, stateStore) -> System.out.println(s + ", " + stateStore));

        final CompoundKeyValueStore<Integer, String, Integer, String> store = // TODO types???
                new CompoundKeyValueStore<>(testDriver.getKeyValueStore(stateStoreName));

        store.range(new CompoundKey<>(103, ""), new CompoundKey<>(103, ""))
                .forEachRemaining(s -> System.out.println(">>> " + s)); // TODO no results, how to range strings???
    }

    @Test
    void rangeWhenHashCollision() {
        final String stateStoreName = "test-state-store";
        final StreamsBuilder builder = new StreamsBuilder();

        final JsonSerializer<KeyValue<String, Integer>> pairSer = new JsonSerializer<>();

        @SuppressWarnings("unchecked") final JsonDeserializer<KeyValue<String, Integer>> pairDes =
                new JsonDeserializer<>((Class<KeyValue<String, Integer>>) (Object) KeyValue.class);

        final Serde<Bytes> byteSerde = Serdes.Bytes();
        final Serde<Integer> intSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<KeyValue<String, Integer>> pairSerde = Serdes.serdeFrom(pairSer, pairDes);

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(stateStoreName), byteSerde, pairSerde));

        final CompoundKeyValueProcessor<String, Integer, Integer> proc =
                new CompoundKeyValueProcessor<>(stateStoreName, s -> s);

        builder.stream("test-topic", Consumed.with(stringSerde, intSerde)).process(() -> proc, stateStoreName);

        final Topology topology = builder.build();

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

        assertEquals("AaBBBB".hashCode(), "BBBBAa".hashCode());

        proc.process("AaBBBB", 1001);
        proc.process("BBBBAa", 1002);
        proc.process("CCCCCC", 1003);

        final CompoundKeyValueStore<String, Integer, String, Integer> store = // TODO types???
                new CompoundKeyValueStore<>(testDriver.getKeyValueStore(stateStoreName));

        store.range(new CompoundKey<>("AaBBBB", 0), new CompoundKey<>("AaBBBB", Integer.MAX_VALUE))
                .forEachRemaining(s -> System.out.println(">>> " + s));
    }
}