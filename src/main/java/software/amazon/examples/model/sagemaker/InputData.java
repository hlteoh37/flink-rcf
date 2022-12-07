package software.amazon.examples.model.sagemaker;

import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
@Builder
public class InputData<T> {
    private List<SagemakerInstances<T>> instances;

    public static <R> InputData<R> from(List<R> data) {
        return InputData.<R>builder()
            .instances(Collections.singletonList(SagemakerInstances.<R>builder()
                .data(SagemakerData.<R>builder()
                    .features(SagemakerFeatures.<R>builder()
                        .values(data)
                        .build())
                    .build())
                .build()))
            .build();
    }
}
