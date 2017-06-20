package com.umantis.poc.kafka.streams.gdpr;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MockMdMService {

	private static final Logger LOGGER = LoggerFactory.getLogger(MockMdMService.class);

	@Autowired
	private EmployeeService employeeService;

	@Value("${gdpr.poc.withEncryption}")
	private Boolean withEncryption;

	@Value("${gdpr.poc.appId.producer}")
	private String producerId;

	@Value("${kafka.servers}")
	private String kafkaServers;

	@Autowired
	@Qualifier("personTopicUsed")
	private String personTopic;

	public List<Future<RecordMetadata>> sendEmployees() throws NoSuchAlgorithmException, NoSuchPaddingException {

		List<Future<RecordMetadata>> futures = new ArrayList<>();

		Properties config = new Properties();
		config.put("client.id", producerId);
		config.put("bootstrap.servers", kafkaServers);
		config.put("acks", "all");
		config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(config);

		if (withEncryption) {
			Cipher cipher = Cipher.getInstance("AES");
			Instant start = Instant.now();
			employeeService.getEmployees().stream().forEach(e -> {
				try {
					String encodedKey = Base64.getEncoder().encodeToString(e.getSecretKey().getEncoded());
					cipher.init(Cipher.ENCRYPT_MODE, e.getSecretKey());
					String encodedString = Base64.getEncoder().encodeToString(cipher.doFinal(e.getSecretInfo().getBytes("UTF-8")));
					producer.send(new ProducerRecord<String, String>(personTopic, e.getId(), encodedKey + "~" + encodedString));
				} catch (IllegalBlockSizeException | BadPaddingException | UnsupportedEncodingException
						| InvalidKeyException e1) {
					e1.printStackTrace();
				}

			});
			Instant end = Instant.now();
			LOGGER.info("Sent {} keys and employee records in {} ms", employeeService.getEmployees().size(),
					Duration.between(start, end).toMillis());
		} else {
			Instant start = Instant.now();
			employeeService.getEmployees().stream()
					.forEach(e -> {
						Future<RecordMetadata> future = producer
								.send(new ProducerRecord<String, String>(personTopic, e.getId(), e.getSecretInfo()));
						futures.add(future);
					});
			Instant end = Instant.now();
			LOGGER.info("Sent {} employee records in {} ms", employeeService.getEmployees().size(),
					Duration.between(start, end).toMillis());
		}


		producer.flush();
		producer.close();

		return futures;

	}

}
