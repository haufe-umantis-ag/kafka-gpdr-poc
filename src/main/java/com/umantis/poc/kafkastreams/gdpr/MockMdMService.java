package com.umantis.poc.kafkastreams.gdpr;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MockMdMService {

	@Autowired
	private EmployeeService employeeService;

	@Value("${kafka.person.topic}")
	private String personTopic;

	@Value("${kafka.keys.topic}")
	private String keyTopic;

	@Value("${gdpr.poc.withEncryption}")
	private Boolean withEncryption;

	@Value("${gdpr.poc.appId.producer}")
	private String producerId;


	public void send() throws NoSuchAlgorithmException, NoSuchPaddingException {

		Properties config = new Properties();
		config.put("client.id", producerId);
		config.put("bootstrap.servers", "host1:9092,host2:9092");
		config.put("acks", "all");
		Producer<String, String> producer = new KafkaProducer<String, String>(config);


		if (withEncryption) {
			Cipher cipher = Cipher.getInstance("AES");
			employeeService.getEmployees().stream().forEach(e -> {
				String encodedKey = Base64.getEncoder().encodeToString(e.getSecretKey().getEncoded());
				producer.send(new ProducerRecord<String, String>(keyTopic, e.getId(), encodedKey));
				try {
					cipher.init(Cipher.ENCRYPT_MODE, e.getSecretKey());
					String encodedString = Base64.getEncoder().encodeToString(cipher.doFinal(e.getSecretInfo().getBytes("UTF-8")));
					producer.send(new ProducerRecord<String, String>(personTopic, e.getId(), encodedString));
				} catch (IllegalBlockSizeException | BadPaddingException | UnsupportedEncodingException
						| InvalidKeyException e1) {
					e1.printStackTrace();
				}

			});
		} else {
			employeeService.getEmployees().stream()
					.forEach(e -> {
						Future<RecordMetadata> future = producer
								.send(new ProducerRecord<String, String>(personTopic, e.getId(), e.getSecretInfo()));
					});
		}

		producer.flush();
		producer.close();

	}

}
