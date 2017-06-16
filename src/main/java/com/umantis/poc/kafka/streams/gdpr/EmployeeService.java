package com.umantis.poc.kafka.streams.gdpr;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class EmployeeService {

	private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeService.class);

	@Value("${gdpr.poc.numOfEmployees}")
	private Integer numOfEmployees;

	@Value("${gdpr.poc.employeeDataSize}")
	private Integer employeeDataSize;

	@Value("${gdpr.poc.withEncryption}")
	private Boolean withEncryption;

	@Autowired
	@Qualifier("AES128KeyGen")
	private KeyGenerator keyGen;

	private List<EmployeeService.Employee> employees = new ArrayList<>();

	@PostConstruct
	private void init() {
		for (int i = 0; i < numOfEmployees; i++) {
			employees.add(new Employee());
			if (i % 10000 == 0) {
				System.out.print(i + " ");
			}
		}
		System.out.println();
		LOGGER.info("Employee list size {}", this.employees.size());
	}

	public List<EmployeeService.Employee> getEmployees() {
		return employees;
	}

	public class Employee {
		private String id;
		private String secretInfo;
		private SecretKey secretKey;

		public Employee() {
			id = UUID.randomUUID().toString();
			secretInfo = RandomStringUtils.randomAlphanumeric(employeeDataSize);
			if (EmployeeService.this.withEncryption) {
				secretKey = EmployeeService.this.keyGen.generateKey();
			}
		}

		public String getId() {
			return id;
		}

		public String getSecretInfo() {
			return secretInfo;
		}

		public SecretKey getSecretKey() {
			return secretKey;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((secretInfo == null) ? 0 : secretInfo.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Employee other = (Employee) obj;
			return this.id.equals(other.id) && this.secretInfo.equals(other.secretInfo);
		}

		private EmployeeService getOuterType() {
			return EmployeeService.this;
		}
		
	}

}
