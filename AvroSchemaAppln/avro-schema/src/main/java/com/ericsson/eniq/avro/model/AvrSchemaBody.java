package com.ericsson.eniq.avro.model;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class AvrSchemaBody {
	private String type;
	private String name;
	private String namespace;
	private List<Field> fields;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public List<Field> getFields() {
		return fields;
	}
	public void setFields(List<Field> fields) {
		this.fields = fields;
	}
	@Override
	public String toString() {
		return "AvrSchemaBody [type=" + type + ", name=" + name + ", namespace=" + namespace + ", fields=" + fields + "]";
	}
	
}
