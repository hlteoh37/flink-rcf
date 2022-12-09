package software.amazon.examples.model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class DaphnetDeserialisationSchema implements DeserializationSchema<DaphnetInput> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long serialVersionUID = 3203704281187526717L;

    @Override
    public DaphnetInput deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, DaphnetInput.class);
    }

    @Override
    public boolean isEndOfStream(DaphnetInput nextElement) {
        return false;
    }

    @Override
    public TypeInformation<DaphnetInput> getProducedType() {
        return TypeInformation.of(DaphnetInput.class);
    }
}
