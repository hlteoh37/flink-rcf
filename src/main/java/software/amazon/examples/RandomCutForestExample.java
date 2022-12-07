package software.amazon.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
public class RandomCutForestExample {
    private static final String STREAM = "LoadTestBeta_Input_27";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, REGION);

        FlinkKinesisConsumer<String> source = new FlinkKinesisConsumer<>(STREAM, new SimpleStringSchema(), consumerConfig);

        env.addSource(source)
            .returns(String.class)
            .print();

        env.execute("Random Cut Forest Example");
    }
}
