package software.amazon.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;
import software.amazon.examples.model.DaphnetDeserialisationSchema;
import software.amazon.examples.model.DaphnetInput;
import software.amazon.examples.model.DaphnetOutput;
import software.amazon.examples.model.DaphnetOutputDeserialisationSchema;
import software.amazon.examples.operators.SagemakerFunction;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition.TRIM_HORIZON;


@Slf4j
public class FileSinkExample {
    private static final String STREAM = "LoadTestBeta_Output_30";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, REGION);
        consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, TRIM_HORIZON.toString());

        // Example for Sine wave

        FlinkKinesisConsumer<DaphnetOutput> source = new FlinkKinesisConsumer<>(STREAM, new DaphnetOutputDeserialisationSchema(), consumerConfig);

        final FileSink<String> sink = FileSink
            .forRowFormat(new Path("/Volumes/workplace/flink-rcf/dahpnet_sagemaker.csv"), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(5))
                    .withInactivityInterval(Duration.ofMinutes(1))
                    .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                    .build())
            .build();


        env.addSource(source)
            .assignTimestampsAndWatermarks(WatermarkStrategy.<DaphnetOutput>forMonotonousTimestamps().withIdleness(Duration.ofMinutes(1L)))
            .map(i -> String.join(",", i.getTimestamp(), String.valueOf(i.getIn_count()), String.valueOf(i.getOut_count()), String.valueOf(i.getIs_anomaly()), String.valueOf(i.getScore())))
                .sinkTo(sink);

        env.execute("Random Cut Forest Example");
    }
}
