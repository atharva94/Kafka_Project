package com.kafkaMicro;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class WikiMediaChangesHandler implements BackgroundEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikiMediaChangesHandler.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public WikiMediaChangesHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        LOGGER.info("Opened connection to Wikimedia stream.");
    }

    @Override
    public void onClosed() throws Exception {
        LOGGER.info("Closed connection to Wikimedia stream.");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        String data = messageEvent.getData();
        LOGGER.info("Received event: {}", data);

        // Send the event data to the Kafka topic
        kafkaTemplate.send(topic, data);
    }

    @Override
    public void onComment(String comment) throws Exception {
        LOGGER.debug("Received comment: {}", comment);
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Error while processing Wikimedia event stream: {}", throwable.getMessage(), throwable);
        // Handle the error or attempt a retry mechanism if necessary
    }
}
