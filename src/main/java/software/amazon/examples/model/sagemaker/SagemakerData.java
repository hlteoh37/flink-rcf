package software.amazon.examples.model.sagemaker;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SagemakerData<T> {
    private SagemakerFeatures<T> features;
}
