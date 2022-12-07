package software.amazon.examples.operators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeAsyncClient;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointRequest;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;
import software.amazon.examples.model.RideRequest;
import software.amazon.examples.model.sagemaker.InputData;
import software.amazon.examples.model.sagemaker.Result;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SagemakerFunction extends RichAsyncFunction<RideRequest, Result> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = MAPPER.writer();

    private transient SageMakerRuntimeAsyncClient client;

    @Override
    public void asyncInvoke(RideRequest rideRequest, ResultFuture<Result> resultFuture) throws Exception {
        final CompletableFuture<InvokeEndpointResponse> result = client.invokeEndpoint(InvokeEndpointRequest.builder()
                .endpointName("jumpstart-example-randomforest-2022-12-07-12-06-47")
                .body(SdkBytes.fromByteArray(WRITER.writeValueAsBytes(InputData.from(
                    ImmutableList.of(rideRequest.getExpectedFare())
                ))))
                .build());

        CompletableFuture.supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                return null;
            }
        }).thenAccept( invokeEndpointResponse -> {
            try {
                resultFuture.complete(Collections.singleton(Result.fromResponse(MAPPER, invokeEndpointResponse)));
            } catch (IOException e) {
                resultFuture.completeExceptionally(e);
            }
        });
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        client = SageMakerRuntimeAsyncClient.builder()
            .region(Region.EU_WEST_2)
            .build();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
