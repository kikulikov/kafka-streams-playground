package kafka.streams.compound;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializerDeserializer {

    static class JsonSerializer<T> implements Serializer<T> {

        private Gson gson = new Gson();

        @Override
        public byte[] serialize(String topic, T t) {
            return gson.toJson(t).getBytes(StandardCharsets.UTF_8);
        }
    }

    static class JsonDeserializer<T> implements Deserializer<T> {
        private Gson gson = new Gson();
        private Class<T> deserializedClass;

        public JsonDeserializer(Class<T> deserializedClass) {
            this.deserializedClass = deserializedClass;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void configure(Map<String, ?> map, boolean b) {
            if (deserializedClass == null) {
                deserializedClass = (Class<T>) map.get("serializedClass");
            }
        }

        @Override
        public T deserialize(String s, byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return gson.fromJson(new String(bytes), deserializedClass);
        }
    }
}
