package software.amazon.examples;

import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.examples.model.RideRequest;
import software.amazon.examples.model.RideRequestDeserialisationSchema;
import software.amazon.examples.model.sagemaker.CorrelatedResult;
import software.amazon.examples.operators.SagemakerFunction;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


@Slf4j
public class RandomCutForestExample {
    private static final String STREAM = "LoadTestBeta_Input_27";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(AWSConfigConstants.AWS_REGION, REGION);

        FlinkKinesisConsumer<RideRequest> source = new FlinkKinesisConsumer<>(STREAM, new RideRequestDeserialisationSchema(), consumerConfig);

        DataStream<Double> rides = env.addSource(source)
            .returns(RideRequest.class)
            .map(RideRequest::getExpectedFare);

        DataStream<CorrelatedResult<Double>> results = AsyncDataStream.unorderedWait(rides, new SagemakerFunction<>("jumpstart-example-randomforest-2022-12-07-12-06-47", Region.EU_WEST_2), 60000, TimeUnit.SECONDS);

        results.print();

        env.execute("Random Cut Forest Example");
    }
}
