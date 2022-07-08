package io.confluent.bootcamp.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.bootcamp.Constant;
import io.confluent.bootcamp.interactivequery.InteractiveQueryService;
import io.confluent.bootcamp.rest.model.DashboardInformation;
import io.confluent.bootcamp.rest.model.Status;
import io.confluent.bootcamp.rest.model.TwitterScore;
import io.confluent.bootcamp.streams.Context;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Path("/")
public class InteractiveQueryRestServlet {

    private final InteractiveQueryService service;

    public InteractiveQueryRestServlet() throws Exception {
        service = new InteractiveQueryService(Context.getKafkaStreams());
    }

    @GET
    @Path("/dashboard")
    @Produces(MediaType.APPLICATION_JSON)
    public DashboardInformation getDashboardInformation() throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {
        DashboardInformation dashboardInformation = new DashboardInformation();
        dashboardInformation.setLastScore(service.fetchLatestScore());
        dashboardInformation.setAverageDailyScore(service.fetchAverageLastScoreForDays(1));
        dashboardInformation.setAverageMonthlyScore(service.fetchAverageLastScoreForDays(30));
        dashboardInformation.setNumberOfBlockedTweep(service.getNumberOfBlockedTweep());
        dashboardInformation.setNumberOfTweetAnalyzed(service.getNumberOfTweetAnalyzed());
        return dashboardInformation;
    }

    @GET
    @Path("/fetchLatestScore")
    @Produces(MediaType.APPLICATION_JSON)
    public TwitterScore fetchLatestScore() throws ExecutionException, InterruptedException, TimeoutException {
        return service.doFetchLatestScore();
    }

    @GET
    @Path("fetchAverageLastScoreForDays")
    @Produces(MediaType.APPLICATION_JSON)
    public Double fetchAverageLastScoreForDays(@PathParam("days") int days) throws ExecutionException, InterruptedException, TimeoutException {
        return service.doFetchAverageLastScoreForDays(days);
    }

    @GET
    @Path("getNumberOfTweetAnalyzed")
    @Produces(MediaType.APPLICATION_JSON)
    public Long getNumberOfTweetAnalyzed() {
        return service.doGetNumberOfTweetAnalyzed();
    }

    @GET
    @Path("status")
    @Produces(MediaType.APPLICATION_JSON)
    public Status getStatus() throws ExecutionException, InterruptedException {
        AdminClient adminClient = Context.getAdminClient();
        String applicationId = Context.getConfiguration().get(StreamsConfig.APPLICATION_ID_CONFIG);
        var groupDescriptionMap = adminClient.describeConsumerGroups(Collections.singleton(applicationId)).all().get();
        var groupDescription = groupDescriptionMap.get(applicationId);
        Status status = new Status();
        status.setNumberOfConsumer(groupDescription.members().size());
        status.setState(groupDescription.state().toString());

        long topic_sum_offset = 0;
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = adminClient.listConsumerGroupOffsets(applicationId).partitionsToOffsetAndMetadata().get();
        for (var entry : groupOffsets.entrySet()) {
            if (entry.getKey().topic().equals(Constant.TOPIC_TWEET)) {
                topic_sum_offset += entry.getValue().offset();
            }
        }
        status.setSummedOffset(topic_sum_offset);
        return status;
    }
}

