package com.ericsson.oss.eniq.techpack.controller;

import com.ericsson.oss.eniq.techpack.response.ResponseData;
import com.ericsson.oss.eniq.techpack.service.interfaces.FeatureDecryptUnzipService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/feature-installer/api")
public class FeatureDecryptUnzipController {
    @Autowired
    private FeatureDecryptUnzipService featureDecryptUnzipService;

    /**
     * The Constant logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(FeatureDecryptUnzipController.class);

    /**
     * This method is used to decrypt the featureZip file and unzip the featureZip file
     *
     * @return derypted and unzipped feature file
     */
    @GetMapping(value = "/v1/install", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> decryptFeatureFile() {
        logger.info("Inside FeatureDecryptUnzipController :decryptFeatureFile()");
        ResponseData response = new ResponseData();
        String fileRead = featureDecryptUnzipService.getDecryptFeatureFile();
        response.setMessage(fileRead);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
