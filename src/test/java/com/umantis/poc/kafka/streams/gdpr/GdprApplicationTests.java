package com.umantis.poc.kafka.streams.gdpr;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;

import javax.crypto.NoSuchPaddingException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.umantis.poc.kafka.streams.gdpr.admin.KafkaAdminUtils;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GdprApplicationTests {

	private static final Logger LOGGER = LoggerFactory.getLogger(GdprApplicationTests.class);

	@Value("${gdpr.poc.retentionTimeInMs}")
	private Integer retentionTimeInMs;

	@Value("${gdpr.poc.numOfEmployees}")
	private Integer numOfEmployees;

	@Value("${gdpr.poc.withEncryption}")
	private Boolean withEncryption;

	@Autowired
	private EmployeeService employeeService;

	@Autowired
	private MockMdMService mdm;

	@Autowired
	private MockStreamProcessor proc;

	@Autowired
	private KafkaAdminUtils adminUtils;

	@Autowired
	@Qualifier("keyTopicUsed")
	private String keyTopic;

	@Autowired
	@Qualifier("personTopicUsed")
	private String personTopic;

	@Before
	public void setup() {
		if (!adminUtils.topicExists(personTopic)) {
			adminUtils.createTopic(personTopic, retentionTimeInMs);
		}
		if (withEncryption) {
			if (!adminUtils.topicExists(keyTopic)) {
				adminUtils.createTopic(keyTopic, retentionTimeInMs);
			}
		}
	}

	@After
	public void tearDown() {
		if (adminUtils.topicExists(personTopic)) {
			adminUtils.markTopicForDeletion(personTopic);
		}
		if (withEncryption) {
			if (adminUtils.topicExists(keyTopic)) {
				adminUtils.markTopicForDeletion(keyTopic);
			}
		}
	}

	@Test
	public void employees() throws NoSuchAlgorithmException, NoSuchPaddingException, InterruptedException {
		Assert.assertEquals(numOfEmployees.intValue(), employeeService.getEmployees().size());
		List<Future<RecordMetadata>> employeesSent = mdm.sendEmployees();
		// Thread.sleep(Math.min(10000, numOfEmployees / 10000 * 1000));
		Assert.assertTrue(employeesSent.parallelStream().allMatch(f -> f.isDone() && !f.isCancelled()));
		LOGGER.info("All employee data sent without cancellation");

		Instant startReceive = Instant.now();
		if (withEncryption) {
			proc.receiveWithEncryption();
		} else {
			proc.receiveWithoutEncryption();
		}
		LOGGER.info("Receive started");
		do {
			Thread.sleep(5000);
			Instant now = Instant.now();
			Duration sp = Duration.between(startReceive, now);
			long processed = proc.getEmployeesProcessed();
			LOGGER.info("Progress {}/{} employees in {} ms, {} employees/sec", processed, numOfEmployees, sp.toMillis(),
					(double) processed / (double) sp.getSeconds());
		} while (!proc.areWeThereYet());

		Assert.assertEquals(numOfEmployees.intValue(), proc.getPersonsStore().approximateNumEntries());
		if (withEncryption) {
			Assert.assertEquals(numOfEmployees.intValue(), proc.getKeysStore().approximateNumEntries());
		}
		LOGGER.info("Receive done in {} ms", proc.spent().toMillis());
	}

}
