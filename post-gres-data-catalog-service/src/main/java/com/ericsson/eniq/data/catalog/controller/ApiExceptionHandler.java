package com.ericsson.eniq.data.catalog.controller;

import com.ericsson.eniq.data.catalog.error.ApiErrorResponse;
import com.ericsson.eniq.data.catalog.error.TechPackNamesMissingException;
import com.ericsson.eniq.data.catalog.error.TechPackVersionMissingException;
import com.ericsson.eniq.data.catalog.error.UnknownFieldException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(TechPackNamesMissingException.class)
    public ResponseEntity<ApiErrorResponse> handleMissingTPName(){
        ApiErrorResponse resp = new ApiErrorResponse(TechPackNamesMissingException.ERROR,
                TechPackNamesMissingException.MESSAGE);
        return new ResponseEntity<>(resp, HttpStatus.BAD_REQUEST);

    }

    @ExceptionHandler(TechPackVersionMissingException.class)
    public ResponseEntity<ApiErrorResponse> handleMissingTPVersion(){
        ApiErrorResponse resp = new ApiErrorResponse(TechPackVersionMissingException.ERROR,
                TechPackVersionMissingException.MESSAGE);
        return new ResponseEntity<>(resp, HttpStatus.BAD_REQUEST);

    }

    @ExceptionHandler(UnknownFieldException.class)
    public ResponseEntity<ApiErrorResponse> handleUnknownField(UnknownFieldException ex){

        ApiErrorResponse resp = new ApiErrorResponse(UnknownFieldException.ERROR,
                ex.getMessage());
        return new ResponseEntity<>(resp, HttpStatus.BAD_REQUEST);

    }
}
