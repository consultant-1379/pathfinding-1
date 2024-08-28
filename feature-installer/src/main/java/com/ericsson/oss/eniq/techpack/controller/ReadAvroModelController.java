package com.ericsson.oss.eniq.techpack.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ericsson.oss.eniq.techpack.model.AvroModel;
import com.ericsson.oss.eniq.techpack.service.ReadAvroModelServiceImpl;

/**
 * The Class Reading Avro Model
 * 
 *
 */
@RestController
public class ReadAvroModelController {
	@Autowired
	private ReadAvroModelServiceImpl avroService;
	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(ReadAvroModelController.class);

	/**
	 * Gets the read avro model file.
	 *
	 * @return the avro model file.
	 */

	@GetMapping("/readAvroModel")
	public ResponseEntity<AvroModel> readAvroModel() {
        logger.info("Inside ReadAvroModelController :readAvroModel()");

		AvroModel avroModel = avroService.readAvroModel();
	
		return new ResponseEntity<>(avroModel, HttpStatus.OK);

	}

}
