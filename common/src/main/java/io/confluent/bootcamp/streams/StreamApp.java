package io.confluent.bootcamp.streams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.ejml.simple.UnsupportedOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class StreamApp {
    private static Logger logger = LoggerFactory.getLogger(StreamApp.class.getName());
    protected Properties properties;
    protected KafkaStreams kafkaStreams;

    protected void run(String[] args) throws Exception {
        run(args, null);
    }

    protected void run(String[] args, Properties extraProperties) throws Exception {
        properties = new Properties();
        if (args.length > 0) {
            properties.load(new FileInputStream(args[0]));
        } else {
            // If no file is provided, try to load kafka.properties from the classpath
            properties.load(new StreamApp().getClass().getResourceAsStream("/kafka.properties"));
        }

        if (extraProperties != null) {
            extraProperties.forEach((k, v) -> {
                properties.put(k, v);
            });
        }

        properties.forEach((k, v) -> {
            Context.getConfiguration().put(k.toString(), v.toString());
        });

        StreamsBuilder builder = new StreamsBuilder();
        buildTopology(builder);
        Topology topology = builder.build();
        logger.info(topology.describe().toString());

        kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.setUncaughtExceptionHandler((e) -> {
            logger.error(null, e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        Context.setKafkaStreams(kafkaStreams);
    }

    protected void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
        throw new UnsupportedOperation("Not implemented");
    }
}
