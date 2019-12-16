package kafka.streams.compound;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.NoSuchElementException;

class CompoundKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    private final KeyValueIterator<Bytes, KeyValue<K, V>> underlying;

    CompoundKeyValueIterator(final KeyValueIterator<Bytes, KeyValue<K, V>> underlying) {
        this.underlying = underlying;
    }

    @Override
    public void close() {
        if (underlying != null) {
            underlying.close();
        }
    }

    @Override
    public K peekNextKey() {
        throw new UnsupportedOperationException("peekNextKey not supported");
    }

    @Override
    public boolean hasNext() {
        return underlying.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        // TODO handle null pointer exception
        return underlying.next().value;
    }
}