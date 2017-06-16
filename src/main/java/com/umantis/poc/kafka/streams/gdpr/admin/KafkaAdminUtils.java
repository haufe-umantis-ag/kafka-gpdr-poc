package com.umantis.poc.kafka.streams.gdpr.admin;

import java.util.List;

/**
 * @author David Espinosa.
 */
public interface KafkaAdminUtils {

    String KAFKA_RETENTION_TIME_PROPERTY = "retention.ms";

    /**
     * Marks a topic for deletion
     *
     * @param topic
     */
    void markTopicForDeletion(String topic);

    /**
     * Retrieves retention.ms property if set, else returns -1
     *
     * @param topic
     * @return
     */
    long getTopicRetentionTime(String topic);

    /**
     * Sets retention.ms topic property
     *
     * @param topic
     * @param retentionTimeInMs
     */
    void setTopicRetentionTime(String topic, long retentionTimeInMs);

    /**
     * Creates a topic
     * auto.create.topics.enable must be set to True (enabled by default)
     *
     * @param topic
     * @param retentionTimeInMs
     */
    void createTopic(String topic, long retentionTimeInMs);

    /**
     * True if topic exists
     *
     * @param topic
     * @return
     */
    boolean topicExists(String topic);

    /**
     * Lists all topics
     *
     * @return
     */
    List<String> listTopics();
}
