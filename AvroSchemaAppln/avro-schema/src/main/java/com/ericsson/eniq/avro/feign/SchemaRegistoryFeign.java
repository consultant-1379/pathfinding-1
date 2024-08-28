package com.ericsson.eniq.avro.feign;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.ericsson.eniq.avro.config.URIConstant;
import com.ericsson.eniq.avro.model.AvrSubject;

@Component
public class SchemaRegistoryFeign {
	@Value("${schema.registry.url}")
	private String baseUrl;

	public AvrSubject getLatestSchemas(String subjectName) {
		RestTemplate restTemplate = new RestTemplate();
		ResponseEntity<AvrSubject> response = restTemplate.getForEntity(baseUrl + URIConstant.SCHEMA_GET_LATEST,
				AvrSubject.class, subjectName);
		return response.getBody();
	}

	public List<String> getAllSubjects() {
		RestTemplate restTemplate = new RestTemplate();
		ResponseEntity<String[]> response = restTemplate.getForEntity(baseUrl + URIConstant.SCHEMA_GET_ALL_SUBJECT,
				String[].class);
		return Arrays.asList(response.getBody());
	}

	public ResponseEntity<String> addSchema(String subjectName, String avrSchema) {
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", "application/json");
		HttpEntity<String> request = new HttpEntity<String>(avrSchema, headers);
		ResponseEntity<String> response = restTemplate.postForEntity(baseUrl + URIConstant.ADD_SCHEMA, request,
				String.class, subjectName);
		return response;
	}

	public void deleteSchema(String subjectName) {
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.delete(baseUrl + URIConstant.DELETE_SCHEMA, subjectName);
	}
}
