package kafka.streams.compound;

import kafka.streams.compound.ByteMethods.ByteKey;
import org.apache.kafka.common.utils.Bytes;

// https://stackoverflow.com/questions/24949676/difference-between-partition-key-composite-key-and-clustering-key-in-cassandra
// PK - The Partition Key is responsible for data distribution across your nodes.
// CK - The Clustering Key is responsible for data sorting within the partition.
public class CompoundKey<PK, CK> {

    private final PK partitionKey;
    private final CK clusteringKey;

    public CompoundKey(PK partitionKey, CK clusteringKey) {
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
    }

    public Bytes binaryKey() {
        return ByteKey.toBinaryKey(partitionKey.hashCode(), clusteringKey.hashCode());
    }

    // TODO should include usual java rubbish here?
    // TODO should be serializable?

    public PK getPartitionKey() {
        return partitionKey;
    }

    public CK getClusteringKey() {
        return clusteringKey;
    }
}
