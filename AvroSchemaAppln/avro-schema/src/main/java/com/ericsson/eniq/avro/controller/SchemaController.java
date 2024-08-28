package com.ericsson.eniq.avro.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.ericsson.eniq.avro.model.AvrSubject;
import com.ericsson.eniq.avro.service.SchemaServices;

@RestController
public class SchemaController {
	private static final Logger LOG = LoggerFactory.getLogger(SchemaController.class);

	@Autowired
	private SchemaServices schemaServices;
	@PostMapping("/create-schema-approach1")
	public List<String> createAvroSchemasApproach1(@RequestBody String techPack) {
		return schemaServices.createAvroSchemasApproach1(techPack);
	} 
	
	@PostMapping("/create-schema")
	public List<String> createAvroSchemas(@RequestBody String techPack) {
		return schemaServices.createAvroSchemas(techPack);
	}

	@GetMapping(value = "/subjects")
	public List<String> getAllSubjects() {
		List<String> response = schemaServices.getAllSubjects();
		return response;
	}

	@GetMapping(value = "/subjects/{subjectName}")
	public AvrSubject getSchema(@PathVariable("subjectName") String subjectName) {
		AvrSubject response = schemaServices.getSchema(subjectName);
		return response;
	}

	@DeleteMapping(value = "/subjects/{subjectName}")
	public String delteSubject(@PathVariable("subjectName") String subjectName) {
		return schemaServices.deleteSubject(subjectName);
	}
	@DeleteMapping(value = "/subjects/")
	public String delteAllSubjects() {
		return schemaServices.deleteAllSubjects();
	}
}
