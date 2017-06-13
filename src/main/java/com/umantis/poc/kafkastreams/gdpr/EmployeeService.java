package com.umantis.poc.kafkastreams.gdpr;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.apache.commons.lang.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class EmployeeService {

	@Value("${gdpr.poc.numOfEmployees}")
	private Integer numOfEmployees;

	@Value("${gdpr.poc.withEncryption}")
	private Boolean withEncryption;

	@Autowired
	@Qualifier("AES128KeyGen")
	private KeyGenerator keyGen;

	private List<EmployeeService.Employee> employees = new ArrayList<>();

	public EmployeeService() {
		for (int i = 0; i < numOfEmployees; i++) {
			employees.add(new Employee());
		}
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
			secretInfo = RandomStringUtils.randomAlphanumeric(512);
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
