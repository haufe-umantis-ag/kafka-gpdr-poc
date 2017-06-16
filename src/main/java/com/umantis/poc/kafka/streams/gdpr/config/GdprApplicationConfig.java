package com.umantis.poc.kafka.streams.gdpr.config;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Properties;

import javax.crypto.KeyGenerator;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GdprApplicationConfig {

	@Value("${kafka.servers}")
	private final String bootstrapServers = null;

	@Value("${gdpr.poc.appId}")
	private final String appId = null;

	@Value("${kafka.person.topic}")
	private String personTopic;

	@Value("${kafka.key.topic}")
	private String keyTopic;

	@Bean("personTopicUsed")
	public String personTopicUsed() {
		return personTopic + "." + RandomStringUtils.randomAlphabetic(8);
	}

	@Bean("keyTopicUsed")
	public String keyTopicUsed() {
		return keyTopic + "." + RandomStringUtils.randomAlphabetic(8);
	}

	@Bean("AES128KeyGen")
	public KeyGenerator keyGenerator() throws NoSuchAlgorithmException {
		KeyGenerator keyGen = KeyGenerator.getInstance("AES");
		SecureRandom random = new SecureRandom();
		keyGen.init(128, random);
		return keyGen;
	}

	@Bean("StreamsConfig")
	public Properties init() {
		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique
		// in the Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// Specify default (de)serializers for record keys and for record
		// values.
		streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1 * 1000);
		// For illustrative purposes we disable record caches
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		return streamsConfiguration;
	}

}
