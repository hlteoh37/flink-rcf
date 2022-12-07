package software.amazon.examples.model.sagemaker;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CorrelatedResult<T> {
    private Result result;
    private T input;
}
