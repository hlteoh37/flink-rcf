package software.amazon.examples.model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class SineWaveDeserialisationSchema implements DeserializationSchema<SineWave> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long serialVersionUID = 3203704281187526717L;

    @Override
    public SineWave deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, SineWave.class);
    }

    @Override
    public boolean isEndOfStream(SineWave nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SineWave> getProducedType() {
        return TypeInformation.of(SineWave.class);
    }
}
