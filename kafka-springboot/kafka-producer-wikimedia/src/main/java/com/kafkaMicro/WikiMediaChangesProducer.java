package com.kafkaMicro;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import okhttp3.HttpUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikiMediaChangesProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikiMediaChangesProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikiMediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {
        String topic = "wikimedia_recentchange";

        BackgroundEventHandler eventHandler = new WikiMediaChangesHandler(kafkaTemplate, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder urlBuilder = new EventSource.Builder(URI.create(url));

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, urlBuilder);
        BackgroundEventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
