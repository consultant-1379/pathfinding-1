package com.ericsson.eniq.data.catalog.error;

public class TechPackVersionMissingException extends RuntimeException{

    public static final  String ERROR = "Request Parameter techPackVersion is missing";
    public static final  String MESSAGE = "TechPack version must be specified to busy hour details";
}
