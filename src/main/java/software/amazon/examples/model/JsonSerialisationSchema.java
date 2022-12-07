package software.amazon.examples.model;

import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerialisationSchema<T> implements SerializationSchema<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long serialVersionUID = -3295778231188890746L;

    @Override
    public byte[] serialize(T element) {
        try {
            return MAPPER.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
