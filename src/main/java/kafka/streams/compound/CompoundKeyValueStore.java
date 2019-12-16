package kafka.streams.compound;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

// TODO https://github.com/axbaretto/kafka/blob/master/streams/src/main/java/org/apache/kafka/streams/state/internals/CompositeReadOnlyKeyValueStore.java
//
public class CompoundKeyValueStore<PK, CK, K, V> {
    // TODO should extend ReadOnlyKeyValueStore? -> hum, doesn't really work (iterators return compound keys)...
    // implements ReadOnlyKeyValueStore<CompoundKey<PK, CK>, KeyValue<K, V>> {

    // TODO store name perhaps?
    private final ReadOnlyKeyValueStore<Bytes, KeyValue<K, V>> stateStore;

    public CompoundKeyValueStore(ReadOnlyKeyValueStore<Bytes, KeyValue<K, V>> stateStore) {
        this.stateStore = stateStore;
    }

//    public V get(CompoundKey<PK, CK> key) {
//        // TODO null pointer exception
//        return stateStore.get(key.binaryKey()).value;
//    }

    // @Override
    public KeyValueIterator<K, V> range(CompoundKey<PK, CK> from, CompoundKey<PK, CK> to) {
        return new CompoundKeyValueIterator<>(stateStore.range(from.binaryKey(), to.binaryKey()));
    }

    // @Override
    public KeyValueIterator<K, V> all() {
        return new CompoundKeyValueIterator<>(stateStore.all());
    }

    // @Override
    public long approximateNumEntries() {
        return stateStore.approximateNumEntries();
    }
}

// TODO applicable for KStream ?
// TODO applicable for KTable ?

//    private static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
//        return asStream(sourceIterator, false);
//    }
//
//    private static <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
//        Iterable<T> iterable = () -> sourceIterator;
//        return StreamSupport.stream(iterable.spliterator(), parallel);
//    }
