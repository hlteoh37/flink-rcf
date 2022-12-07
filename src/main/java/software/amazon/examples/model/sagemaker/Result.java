package software.amazon.examples.model.sagemaker;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import java.io.IOException;
import java.util.List;

@Data
@NoArgsConstructor
public class Result {
    private List<SagemakerScore> scores;

    public static Result fromResponse(ObjectMapper mapper, InvokeEndpointResponse response) throws IOException {
        return mapper.readValue(response.body().asByteArray(), Result.class);
    }
}
