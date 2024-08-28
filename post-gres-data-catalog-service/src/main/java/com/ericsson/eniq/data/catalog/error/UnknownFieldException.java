package com.ericsson.eniq.data.catalog.error;

public class UnknownFieldException extends RuntimeException{

    public static final  String ERROR = "Request Parameter is unknown";
    private static final  String MESSAGE = "Field %s in the request is not known";
    private final String fieldName;

    public UnknownFieldException(String fieldName){
        super();
        this.fieldName = fieldName;
    }

    @Override
    public  String getMessage() {
        return String.format(MESSAGE,fieldName);
    }
}
