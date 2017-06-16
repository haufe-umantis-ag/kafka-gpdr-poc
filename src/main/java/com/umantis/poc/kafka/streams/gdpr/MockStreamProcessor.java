package com.umantis.poc.kafka.streams.gdpr;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MockStreamProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(MockStreamProcessor.class);

	@Autowired
	@Qualifier("StreamsConfig")
	private Properties streamsConfig;

	@Autowired
	@Qualifier("keyTopicUsed")
	private String keyTopic;

	@Autowired
	@Qualifier("personTopicUsed")
	private String personTopic;

	@Value("${gdpr.poc.withEncryption}")
	private Boolean withEncryption;

	private KeyValueStore<String, String> personStore;
	private KeyValueStore<String, String> keyStore;

	private KStreamBuilder builder;

	private String personStoreName;

	private String keyStoreName;

	Cipher cipher;

	private long employeesProcessed = 0;

	@Value("${gdpr.poc.numOfEmployees}")
	private long numOfEmployees;

	private Instant start, end;

	@PostConstruct
	public void init() throws NoSuchAlgorithmException, NoSuchPaddingException {
		personStoreName = personTopic + "_Store_KStream";
		keyStoreName = null;
		cipher = Cipher.getInstance("AES");
		builder = new KStreamBuilder();

		@SuppressWarnings("rawtypes")
		StateStoreSupplier personStoreSupplier = Stores.create(personStoreName) // there's
																				// a
																				// whole
																				// hierarchy
																				// of
																				// StateStores
				.withKeys(Serdes.String()) // must match the return type of the
											// Transformer's id extractor
				.withValues(Serdes.String()).inMemory().build();
		builder.addStateStore(personStoreSupplier);

		if (withEncryption) {
			keyStoreName = keyTopic + "_Store_KStream";
			@SuppressWarnings("rawtypes")
			StateStoreSupplier keyStoreSupplier = Stores.create(keyStoreName).withKeys(Serdes.String())
					.withValues(Serdes.String()).inMemory().build();
			builder.addStateStore(keyStoreSupplier);
		}
	}

	public void receiveWithoutEncryption() {

		final KStream<String, String> personStream = builder.stream(Serdes.String(), Serdes.String(), personTopic);
		personStream.process(() -> new Processor<String, String>() {

			@Override
			public void init(ProcessorContext context) {
				personStore = (KeyValueStore) context.getStateStore(personStoreName);
				LOGGER.info("Got KeyValueStore {}", personStoreName);
			}

			@Override
			public void process(String key, String value) {
				personStore.put(key, value);
				employeesProcessed++;
				if(areWeThereYet()) {
					end = Instant.now();
				}
			}

			@Override
			public void punctuate(long timestamp) {
				// not implemented now
			}

			@Override
			public void close() {
				personStore.close();
			}

		}, personStoreName);

		// startup
		final KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
		streams.cleanUp();
		streams.start();
		start = Instant.now();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka
		// Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	public void receiveWithEncryption() {

		final KStream<String, String> keyStream = builder.stream(Serdes.String(), Serdes.String(), keyTopic);
		final KStream<String, String> personStream = builder.stream(Serdes.String(), Serdes.String(), personTopic);

		personStream.join(keyStream, (secretInfo, cypher) -> cypher + "~" + secretInfo, JoinWindows.of(100000))
				.process(() -> new Processor<String, String>() {

					@Override
					public void init(ProcessorContext context) {
						personStore = (KeyValueStore) context.getStateStore(personStoreName);
						LOGGER.info("Got KeyValueStore {}", personStoreName);
						keyStore = (KeyValueStore) context.getStateStore(keyStoreName);
						LOGGER.info("Got KeyValueStore {}", keyStoreName);
					}

					@Override
					public void process(String key, String value) {
						String[] cypherAndSecretInfo = value.split("~");
						String encodedCypherKey = cypherAndSecretInfo[0];
						String encodedSecretInfo = cypherAndSecretInfo[1];
						keyStore.put(key, encodedCypherKey);

						try {
							// decoding procedure
							// 1. decode the base64 encoded string
							byte[] decodedKey = Base64.getDecoder().decode(encodedCypherKey);
							// 2. rebuild key using SecretKeySpec
							SecretKey originalKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
							// 3. decode the base64 secretinfo
							cipher.init(Cipher.DECRYPT_MODE, originalKey);
							byte[] encrypted = Base64.getDecoder().decode(encodedSecretInfo);
							String originalSecretInfo = new String(cipher.doFinal(encrypted));
							personStore.put(key, originalSecretInfo);
						} catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						employeesProcessed++;
						if (areWeThereYet()) {
							end = Instant.now();
						}
					}

					@Override
					public void punctuate(long timestamp) {
						// not implemented now
					}

					@Override
					public void close() {
						personStore.close();
					}

				}, personStoreName, keyStoreName);

		// startup
		final KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
		streams.cleanUp();
		streams.start();
		start = Instant.now();

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka
		// Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	public KeyValueStore<String, String> getPersonsStore() {
		return personStore;
	}

	public KeyValueStore<String, String> getKeysStore() {
		return keyStore;
	}

	public boolean areWeThereYet() {
		return !(employeesProcessed < numOfEmployees);
	}

	public long getEmployeesProcessed() {
		return employeesProcessed;
	}

	public Duration spent() {
		return Duration.between(start, end);
	}

}
