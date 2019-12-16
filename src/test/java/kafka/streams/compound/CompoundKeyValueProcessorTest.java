package kafka.streams.compound;

import kafka.streams.compound.JsonSerializerDeserializer.JsonDeserializer;
import kafka.streams.compound.JsonSerializerDeserializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;


class CompoundKeyValueProcessorTest {

    @Test
    void process() {
        final String stateStoreName = "test-state-store";
        final StreamsBuilder builder = new StreamsBuilder();

        final JsonSerializer<KeyValue<Integer, String>> pairSer = new JsonSerializer<>();

        @SuppressWarnings("unchecked")
        // OR Type typeOfListOfFoo = new TypeToken<List<Foo>>(){}.getType()
        final JsonDeserializer<KeyValue<Integer, String>> pairDes =
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

        final Integer keyToAssert = 42;
        final String valueToAssert = "bunch of monkeys";
        proc.process(keyToAssert, valueToAssert);

        final KeyValueStore<Bytes, KeyValue<Integer, String>> ss = testDriver.getKeyValueStore(stateStoreName);

        testDriver.getKeyValueStore(stateStoreName);

        final KeyValueIterator<Bytes, KeyValue<Integer, String>> it = ss.all();
        final KeyValue<Bytes, KeyValue<Integer, String>> value = it.next();

        assertEquals(valueToAssert, value.value.value);
        assertEquals(keyToAssert, value.value.key); // TODO key: Double ???
    }
}