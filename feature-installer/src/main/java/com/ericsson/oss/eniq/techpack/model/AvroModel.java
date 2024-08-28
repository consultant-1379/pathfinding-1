package com.ericsson.oss.eniq.techpack.model;

import java.io.Serializable;
import java.util.List;

/**
 * 
 * 
 * The class AvroModel
 */
public class AvroModel implements Serializable {
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;
	
	/** The avromodel type. */
      private String type;
      
  	/** The avromodel namespace. */
	private String namespace;
	
	/** The avromodel name. */
	private String name;
	
	/** The avromodel list of fields. */
	private List<FieldsModel> fields;

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public AvroModel() {

	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<FieldsModel> getFields() {
		return fields;
	}

	public void setFields(List<FieldsModel> fields) {
		this.fields = fields;
	}

	public String toString() {
		return "AvroModel [type=" + type + ", namespace=" + namespace + ", name=" + name + ", fields=" + fields + "]";
	}

	public void add(List<FieldsModel> fields2) {
		
	}

}
