package com.malsolo.kafka;

import static com.malsolo.kafka.util.CommonAdminAndConfig.TOPIC_NAME;
import static com.malsolo.kafka.util.CommonAdminAndConfig.createTopic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import com.malsolo.kafka.model.DataRecord;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

public class ConsumerExample {

    public static void main(String[] args) {
        var props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        createTopic(TOPIC_NAME, props);

        Long total_count = 0L;
        try (var consumer = new KafkaConsumer<String, DataRecord>(props)) {
            consumer.subscribe(Arrays.asList(TOPIC_NAME));
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                  var key = record.key();
                  var value = record.value();
                  total_count += value.getCount();
                  System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value, total_count);
                }                
            }
        }            
    }
    
}
