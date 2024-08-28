package com.ericsson.oss.eniq.techpack.globalexception;

import com.ericsson.oss.eniq.techpack.response.ErrorResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.WebRequest;

import java.io.IOException;

@ControllerAdvice
public class ExceptionHelper {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionHelper.class);

    @ExceptionHandler(value = {IOException.class})
    @ResponseStatus(value = HttpStatus.OK)
    public ErrorResponse handleIOException(IOException ex) {
        logger.error("IOException ....",ex);
        return getExceptionResponse("Either File not found or error occured during file reading" + ex.getMessage(), HttpStatus.OK.value());
    }


    @ExceptionHandler(value = {JsonProcessingException.class})
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleJsonProcessingException(JsonProcessingException ex) {
        logger.error("JSON Exception ....", ex);
        return getExceptionResponse("Exception Occured During JSON processing" + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR.value());
    }

    @ExceptionHandler(TechPackExceptions.class)
    @ResponseStatus(value = HttpStatus.OK)
    public ErrorResponse handleGenericException(final TechPackExceptions ex, final WebRequest request) {
        logger.error("TechPack Exception ....", ex);
        return getExceptionResponse("TechPack Exception : " + ex.getMessage(), HttpStatus.PARTIAL_CONTENT.value());
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(value = HttpStatus.OK)
    public ErrorResponse handleGenericException(final Exception ex, final WebRequest request) {
        logger.error("handleGenericException ....", ex);
        return getExceptionResponse("Exception " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR.value());
    }

    private ErrorResponse getExceptionResponse(final String message, final Integer errorCode) {
        final ErrorResponse exceptionResponse = new ErrorResponse();
        exceptionResponse.setErrorCode(errorCode.toString());
        exceptionResponse.setErrorDescription(message);
        logger.error("message:{}", exceptionResponse);
        return exceptionResponse;
    }


}
