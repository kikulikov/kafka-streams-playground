package kafka.streams.compound;

import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;

public class ByteMethods {

    public static class ByteKey {
        static final private int INT_SIZE = 4;
        static final private int KEY_SIZE = INT_SIZE * 2;

        static public Bytes toBinaryKey(int leftHash, int rightHash) {
            final ByteBuffer buf = ByteBuffer.allocate(KEY_SIZE);
            buf.putInt(leftHash);
            buf.putInt(rightHash);
            return Bytes.wrap(buf.array());
        }
    }

    static final private int INT_SIZE = 4; // TODO what if key is long?
    static final private int KEY_SIZE = INT_SIZE * 2;

    public static Bytes lowerRangeHigherByte(Object key) {
        ByteBuffer buf = ByteBuffer.allocate(KEY_SIZE);
        buf.putInt(key.hashCode());
        buf.putInt(0);

        return new Bytes(buf.array());
    }

    public static Bytes lowerRangeLowerByte(Object key) {
        ByteBuffer buf = ByteBuffer.allocate(KEY_SIZE);
        buf.putInt(0);
        buf.putInt(key.hashCode());

        return new Bytes(buf.array());
    }

    public static Bytes upperRangeHigherByte(Object key) {
        ByteBuffer buf = ByteBuffer.allocate(KEY_SIZE);
        buf.putInt(key.hashCode());
        buf.putInt(-1); // the range is inclusive, so need to make sure we get 0xId_FFFF

        return new Bytes(buf.array());
    }

    public static Bytes upperRangeLowerByte(Object key) {
        final ByteBuffer buf = ByteBuffer.allocate(KEY_SIZE);
        buf.putInt(-1);
        buf.putInt(key.hashCode());

        return new Bytes(buf.array());
    }
}
