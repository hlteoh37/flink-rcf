package software.amazon.examples.model.sagemaker;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class SagemakerFeatures<T> {
    private List<T> values;
}
