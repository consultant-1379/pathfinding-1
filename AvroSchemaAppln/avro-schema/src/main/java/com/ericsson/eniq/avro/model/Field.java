package com.ericsson.eniq.avro.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Field {

	private String name;
	private Object type;

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

	@Override
	public String toString() {
		return "Field [name=" + name + ", type=" + type + "]";
	}

}
