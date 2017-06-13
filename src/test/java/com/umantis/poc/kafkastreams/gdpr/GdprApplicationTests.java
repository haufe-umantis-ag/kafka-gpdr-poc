package com.umantis.poc.kafkastreams.gdpr;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBootTest
public class GdprApplicationTests {

	@Value("${gdpr.poc.numOfEmployees}")
	private Integer numOfEmployees;

	@Value("${gdpr.poc.withEncryption}")
	private Boolean withEncryption;

	@Autowired
	private EmployeeService employeeService;

	@Test
	public void contextLoads() {
	}

	@Test
	public void employeesCreated() {
		Assert.assertEquals(numOfEmployees.intValue(), employeeService.getEmployees().size());
	}

}
