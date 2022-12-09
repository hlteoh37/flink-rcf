package software.amazon.examples.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class DaphnetInput {
    private String timestamp;
    private int in_count;
    private int out_count;
    private int is_anomaly;
}
