package com.dimabugaev.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "wikimedia.recentchange";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventSource.Builder builder =
                new BackgroundEventSource.Builder(eventHandler, new EventSource.Builder(URI.create(url)));
        BackgroundEventSource eventSource = builder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(5);
    }
}
