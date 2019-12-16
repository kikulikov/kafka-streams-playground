package kafka.streams.compound;

import kafka.streams.compound.ByteMethods.ByteKey;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.function.Function;

public class CompoundKeyValueProcessor<PK, V, CK> implements Processor<PK, V> {

    private final String stateStoreName;
    private final Function<V, CK> clusteringKeyF;
    private KeyValueStore<Bytes, KeyValue<PK, V>> stateStore;
    private ProcessorContext context;

    public CompoundKeyValueProcessor(String stateStoreName, Function<V, CK> clusteringKeyF) {
        this.stateStoreName = stateStoreName;
        this.clusteringKeyF = clusteringKeyF;
    }

    @Override
    @SuppressWarnings("unchecked") // TODO unchecked casting =/
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore<Bytes, KeyValue<PK, V>>) context.getStateStore(stateStoreName);
    }

    @Override
    public void process(PK key, V value) {
        stateStore.put(compoundKey(key, value), new KeyValue<>(key, value));
        context.commit();
    }

    private Bytes compoundKey(PK key, V value) {
        final CK clusteringKey = clusteringKeyF.apply(value);
        return ByteKey.toBinaryKey(key.hashCode(), clusteringKey.hashCode());
    }

    @Override
    public void close() {
        stateStore.close();
    }
}