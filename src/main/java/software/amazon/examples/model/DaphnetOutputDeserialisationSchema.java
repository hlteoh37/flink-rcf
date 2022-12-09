package software.amazon.examples.model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class DaphnetOutputDeserialisationSchema implements DeserializationSchema<DaphnetOutput> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long serialVersionUID = 3203704281187526717L;

    @Override
    public DaphnetOutput deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, DaphnetOutput.class);
    }

    @Override
    public boolean isEndOfStream(DaphnetOutput nextElement) {
        return false;
    }

    @Override
    public TypeInformation<DaphnetOutput> getProducedType() {
        return TypeInformation.of(DaphnetOutput.class);
    }
}
