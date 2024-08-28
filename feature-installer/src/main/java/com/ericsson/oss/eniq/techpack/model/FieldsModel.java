package com.ericsson.oss.eniq.techpack.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
/**
 * 
 * The class Avro model
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldsModel implements Serializable {
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;
	
	/** The avromodel list of fields. */
	private String name;
	private Object type;

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public FieldsModel() {

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Object getType() {
		return type;
	}

	public void setType(Object type) {
		this.type = type;
	}

	public String toString() {
		return "FieldsModel [name=" + name + ", type=" + type + "]";
	}

}
