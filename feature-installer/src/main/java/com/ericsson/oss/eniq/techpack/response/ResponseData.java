package com.ericsson.oss.eniq.techpack.response;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class ResponseData {
    private int Code;
    private String message;
    private Object responseBody;

}
