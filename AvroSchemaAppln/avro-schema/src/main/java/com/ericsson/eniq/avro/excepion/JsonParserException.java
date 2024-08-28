package com.ericsson.eniq.avro.excepion;

public class JsonParserException extends RuntimeException {

	
	private static final long serialVersionUID = 1731941409534594079L;

	public JsonParserException() {
		super();

	}

	public JsonParserException(String message, Throwable cause) {
		super(message, cause);

	}

	public JsonParserException(String message) {
		super(message);

	}

	public JsonParserException(Throwable cause) {
		super(cause);

	}


}
