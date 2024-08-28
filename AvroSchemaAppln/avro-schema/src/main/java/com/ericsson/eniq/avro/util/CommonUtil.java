package com.ericsson.eniq.avro.util;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.ericsson.eniq.avro.excepion.JsonParserException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CommonUtil {

	

	public static String objectToJson(Object object) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new JsonParserException(e);
		}
	}

	public static String getAvroType(String fieldType) {
		String avroType = "string";
		try {
			fieldType = fieldType.toLowerCase();
			if (fieldType.contains("int") || fieldType.contains("num")) {
				avroType = "int";
			} else if (fieldType.contains("date")) {
				avroType = avroType + ",date";
			}
			return avroType;
		} catch (Exception e) {
			throw new JsonParserException(e);
		}
	}

}
