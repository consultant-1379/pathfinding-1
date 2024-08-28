package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.model.AvroModel;

/**
 * The interface ReadAvroModelService
 * 
 * 
 *
 */
public interface ReadAvroModelService {
	/**
	 * Fetch the avro model files
	 *
	 * @return the avro model files.
	 */
	public AvroModel readAvroModel();
}
