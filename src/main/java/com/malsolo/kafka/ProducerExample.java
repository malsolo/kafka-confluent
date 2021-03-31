package com.malsolo.kafka;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.malsolo.kafka.model.DataRecord;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

public class ProducerExample {

    private static final String TOPIC_NAME = "test_topic";

    public static void main(String[] args) {
        var props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");

        createTopic(TOPIC_NAME, props);

        var producer = new KafkaProducer<String, DataRecord>(props);

        // Produce sample data
        var numMessages = 10L;
        for (var i = 0L; i < numMessages; i++) {
          var key = "alice";
          var record = new DataRecord(i);

          System.out.printf("Producing record: %s\t%s%n", key, record);
          producer.send(new ProducerRecord<String, DataRecord>(TOPIC_NAME, key, record), 
            (rm, e) -> System.out.printf("%s",
                e != null ? e.getMessage() : 
                String.format("Produced record to topic %s partition [%d] @ offset %d%n", rm.topic(), rm.partition(), rm.offset())
            )
          );
        }

        producer.flush();

        System.out.printf("10 messages were produced to topic %s%n", TOPIC_NAME);

        producer.close();

    }

    public static void createTopic(final String topic, final Properties props) {
        var newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (var adminClient = AdminClient.create(props)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
          if (!(e.getCause() instanceof TopicExistsException)) {
              throw new RuntimeException(e);
          }
        }
    }
    
}
