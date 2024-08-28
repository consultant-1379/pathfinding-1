package com.ericsson.eniq.data.catalog.error;

public class TechPackNamesMissingException extends RuntimeException{

    public static final  String ERROR = "Request Parameter techPackNames is missing";
    public static final  String MESSAGE = "TechPack name must be specified to get transformation cache details";


}
