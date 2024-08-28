package com.ericsson.oss.eniq.techpack.service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.ericsson.oss.eniq.techpack.model.AvroModel;
import com.google.gson.Gson;

/**
 * The class ReadAvroModelServiceImpl
 * 
 *
 */

@Service
public class ReadAvroModelServiceImpl implements ReadAvroModelService {
	/** The Resource loader. */
	@Autowired
	ResourceLoader resourceLoader;

	/** The Constant logger. */
	final Logger logger = LoggerFactory.getLogger(ReadAvroModelServiceImpl.class);

	public AvroModel readAvroModel() {

		/** Reading avro model file from class path. */
		Resource resource = resourceLoader.getResource("classpath:avromodel.json");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(resource.getFile()));
		} catch (FileNotFoundException e) {
			logger.info("FileNotFoundException Occured  in ReadAvroModelServiceImpl:readAvroModel()" + e.getMessage());
		} catch (IOException e) {
			logger.info("IOException Occured  in ReadAvroModelServiceImpl:readAvroModel()" + e.getMessage());
		}
		Gson gson = new Gson();
		AvroModel avroModel = gson.fromJson(reader, AvroModel.class);
		try {
			reader.close();
		} catch (IOException e) {
			logger.info("IOException Occured  in ReadAvroModelServiceImpl:readAvroModel()" + e.getMessage());
		}
		/** Return avro model. */
		return avroModel;

	}
}
