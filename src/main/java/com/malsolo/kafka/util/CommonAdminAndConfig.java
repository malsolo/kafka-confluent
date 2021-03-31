package com.malsolo.kafka.util;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public abstract class CommonAdminAndConfig {

    public static final String TOPIC_NAME = "test_topic";

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
