package com.ericsson.oss.eniq.techpack.controller;

import com.ericsson.oss.eniq.techpack.response.ResponseData;
import com.ericsson.oss.eniq.techpack.service.interfaces.DecryptTechPackService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/feature-installer/api")
public class FeatureDecryptController {
    private static final Logger logger = LoggerFactory.getLogger(FeatureDecryptController.class);

    @Autowired
    private DecryptTechPackService decryptTechPackService;

    @GetMapping("/v1/decrypt-techpack")
    public ResponseEntity<Object> decryptTechPack() {
        logger.info("Inside decryptTechPack() Method");
        ResponseData responseData = decryptTechPackService.getDecryptFile();
        return new ResponseEntity<>(responseData, HttpStatus.OK);
    }

}
