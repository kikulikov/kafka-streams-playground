package kafka.streams.compound;

import kafka.streams.compound.ByteMethods.ByteKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ByteMethodsTest {

    @Test
    void binaryLowerKeyTest() {
        assertEquals(0L, ByteKey.toBinaryKey(0, 5).get()[3]);
        assertEquals(5L, ByteKey.toBinaryKey(0, 5).get()[7]);
    }

    @Test
    void binaryHigherKeyTest() {
        assertEquals(6L, ByteKey.toBinaryKey(6, 0).get()[3]);
        assertEquals(0L, ByteKey.toBinaryKey(6, 0).get()[7]);
    }

    @Test
    void binaryKeyTest() {
        assertEquals(0L, ByteKey.toBinaryKey(0, Integer.MAX_VALUE).get()[3]);
        assertEquals(127L, ByteKey.toBinaryKey(0, Integer.MAX_VALUE).get()[4]);
        assertEquals(-1L, ByteKey.toBinaryKey(0, Integer.MAX_VALUE).get()[7]);
    }
}