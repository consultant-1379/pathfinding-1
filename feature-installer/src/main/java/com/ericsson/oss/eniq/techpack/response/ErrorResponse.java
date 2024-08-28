package com.ericsson.oss.eniq.techpack.response;

import lombok.Data;

@Data
public class ErrorResponse {

    private String errorCode;
    private String errorDescription;

}
