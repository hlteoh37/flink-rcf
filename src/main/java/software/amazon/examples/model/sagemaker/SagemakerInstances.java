package software.amazon.examples.model.sagemaker;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SagemakerInstances<T> {
    private SagemakerData<T> data;
}
