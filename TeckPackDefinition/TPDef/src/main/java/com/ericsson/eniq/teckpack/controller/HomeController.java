package com.ericsson.eniq.teckpack.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.ericsson.eniq.teckpack.serviceImpl.CreateCliImpl;
@RestController
public class HomeController {
	@Autowired
	private CreateCliImpl createCliImpl;

	@GetMapping("/")
	public String welcomeMsg() {
		return "Welcome to Json generator application";
	}

	@PostMapping("/generate-json-approach1")
	public ResponseEntity<String> transformToJsonByApproach1() {
		String res = createCliImpl.convertByApproach1();
		return new ResponseEntity<String>(res, HttpStatus.CREATED);
	}
	
	@PostMapping("/generate-json-approach2")
	public ResponseEntity<String> transformToJsonByApproach2() {
		String res = createCliImpl.convertByApproach2();
		return new ResponseEntity<String>(res, HttpStatus.CREATED);
	}
	@PostMapping("/tpm-to-json")
	public ResponseEntity<String> tpmToJson() {
		String res = createCliImpl.tpmToJson();
		return new ResponseEntity<String>(res, HttpStatus.CREATED);
	}
	
	@PostMapping("/generate-json")
	public ResponseEntity<String> generateJson() {
		String res = createCliImpl.generateJson();
		return new ResponseEntity<String>(res, HttpStatus.CREATED);
	}

}
