package software.amazon.examples;

import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;
import software.amazon.examples.model.DaphnetDeserialisationSchema;
import software.amazon.examples.model.DaphnetInput;
import software.amazon.examples.model.DaphnetOutput;
import software.amazon.examples.model.JsonSerialisationSchema;
import software.amazon.examples.model.SineWave;
import software.amazon.examples.model.SineWaveDeserialisationSchema;
import software.amazon.examples.model.SineWaveResult;
import software.amazon.examples.operators.SagemakerFunction;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition.TRIM_HORIZON;


@Slf4j
public class DaphnetExample {
    private static final String STREAM = "LoadTestBeta_Input_30";
    private static final String OUTPUT_STREAM = "LoadTestBeta_Output_30";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, REGION);
        consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, TRIM_HORIZON.toString());

        // Example for Sine wave

        FlinkKinesisConsumer<DaphnetInput> source = new FlinkKinesisConsumer<>(STREAM, new DaphnetDeserialisationSchema(), consumerConfig);
        KinesisStreamsSink<DaphnetOutput> sink = KinesisStreamsSink.<DaphnetOutput>builder()
            .setKinesisClientProperties(consumerConfig)
            .setSerializationSchema(new JsonSerialisationSchema<>())
            .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
            .setStreamName(OUTPUT_STREAM)
            .build();

        DataStream<DaphnetInput> wave = env.addSource(source)
            .returns(DaphnetInput.class);

        DataStream<DaphnetOutput> results = AsyncDataStream.unorderedWait(wave, new SagemakerFunction<>("daphnet", s -> ImmutableList.of(s.getIn_count(), s.getOut_count())), 60000, TimeUnit.SECONDS)
            .map(res -> DaphnetOutput.builder()
                .in_count(res.getInput().getIn_count())
                .out_count(res.getInput().getOut_count())
                .is_anomaly(res.getInput().getIs_anomaly())
                .score(res.getResult().getScores().get(0).getScore())
                .timestamp(res.getInput().getTimestamp())
                .build());

        results.print();

        results.sinkTo(sink);

        env.execute("Random Cut Forest Example");
    }
}
