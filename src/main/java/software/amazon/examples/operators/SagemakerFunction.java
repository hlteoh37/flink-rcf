package software.amazon.examples.operators;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
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
import software.amazon.examples.model.sagemaker.CorrelatedResult;
import software.amazon.examples.model.sagemaker.InputData;
import software.amazon.examples.model.sagemaker.Result;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SagemakerFunction<IN, TYP, VAL extends List<TYP>> extends RichAsyncFunction<IN, CorrelatedResult<IN>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = MAPPER.writer();

    private final String endpointName;
    private final MapFunction<IN, VAL> mapFunction;

    private transient SageMakerRuntimeAsyncClient client;

    public SagemakerFunction(String endpointName, MapFunction<IN, VAL> mapFunction) {
        this.endpointName = endpointName;
        this.mapFunction = mapFunction;
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<CorrelatedResult<IN>> resultFuture) throws Exception {
//        System.out.println("INVOKED");
        final CompletableFuture<InvokeEndpointResponse> result = client.invokeEndpoint(InvokeEndpointRequest.builder()
            .endpointName(endpointName)
            .body(SdkBytes.fromByteArray(WRITER.writeValueAsBytes(InputData.from(
                mapFunction.map(input)
            ))))
            .build());

        CompletableFuture.supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                return null;
            }
        }).thenAccept(invokeEndpointResponse -> {
            try {
                resultFuture.complete(Collections.singleton(
                    CorrelatedResult.<IN>builder()
                        .input(input)
                        .result(Result.fromResponse(MAPPER, invokeEndpointResponse))
                        .build()));
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
