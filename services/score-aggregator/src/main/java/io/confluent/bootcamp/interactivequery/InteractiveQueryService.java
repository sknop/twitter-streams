package io.confluent.bootcamp.interactivequery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.bootcamp.Constant;
import io.confluent.bootcamp.streams.Context;
import io.confluent.bootcamp.streams.interactivequery.HostStoreInfo;
import io.confluent.bootcamp.streams.interactivequery.MetadataService;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.client.HttpClient;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class InteractiveQueryService {
    private final ReadOnlyKeyValueStore<Long, Object> storeScore;
    private final MetadataService metadataService;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public InteractiveQueryService(KafkaStreams kafkaStreams) throws Exception {
        storeScore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(Constant.STORE_DUMMY, QueryableStoreTypes.keyValueStore()));
        metadataService = new MetadataService(Context.getKafkaStreams());
        httpClient = new HttpClient();
        httpClient.start();
        objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
    }

    public io.confluent.bootcamp.rest.model.TwitterScore fetchLatestScore() throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {
        HostStoreInfo hostStoreInfo = metadataService.streamsMetadataForStoreAndKey(Constant.STORE_DUMMY, 0L, new LongSerializer());
        throw new NotImplementedException("");
    }

    public io.confluent.bootcamp.rest.model.TwitterScore doFetchLatestScore() {
        throw new NotImplementedException("");
    }

    public double fetchAverageLastScoreForDays(int numberOfDays) throws ExecutionException, InterruptedException, TimeoutException {
        HostStoreInfo hostStoreInfo = metadataService.streamsMetadataForStoreAndKey(Constant.STORE_DUMMY, 0L, new LongSerializer());
        throw new NotImplementedException("");
    }

    public double doFetchAverageLastScoreForDays(int numberOfDays) {
        throw new NotImplementedException("");
    }

    public long getNumberOfBlockedTweep() {
        throw new NotImplementedException("");
    }

    public long getNumberOfTweetAnalyzed() throws ExecutionException, InterruptedException, TimeoutException {
        HostStoreInfo hostStoreInfo = metadataService.streamsMetadataForStoreAndKey(Constant.STORE_DUMMY, 0L, new LongSerializer());
        throw new NotImplementedException("");
    }

    public long doGetNumberOfTweetAnalyzed() {
        throw new NotImplementedException("");
    }
}
