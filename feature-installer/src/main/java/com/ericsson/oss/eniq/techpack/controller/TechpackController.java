package com.ericsson.oss.eniq.techpack.controller;

import com.ericsson.oss.eniq.techpack.response.ResponseData;
import com.ericsson.oss.eniq.techpack.service.interfaces.TechpackPreCheckService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/v1/techpack")
public class TechpackController {
    private Logger logger = LoggerFactory.getLogger(TechpackController.class);

    @Autowired
    private TechpackPreCheckService techpackPreCheckService;
    /**
     * This function will perform TechPack installation precheck.
     * @param checkForRequiredTechPacks  boolean
     * @return responseData
     */
    @GetMapping(value = "/tp-dependencies-precheck/{checkForRequiredTechPacks}", produces = "application/json")
    public ResponseEntity<Object> tpDependenciesPreCheck(@PathVariable("checkForRequiredTechPacks") String checkForRequiredTechPacks) throws Exception {
        logger.trace("Inside tpDependenciesPrecheck() method");
        ResponseData responseData = new ResponseData();

        if (checkForRequiredTechPacks.equalsIgnoreCase("true")) {
            Map<String, String> requiredTechPackInstallations = techpackPreCheckService.readVersionPropertyFile();
            logger.info("Checking for required tech packs.");
            techpackPreCheckService.checkRequiredTechPackInstallations(requiredTechPackInstallations);
            responseData.setMessage("Checking for required tech packs finished.");
            logger.info("Checking for required tech packs finished.");
            return new ResponseEntity<>(responseData, HttpStatus.OK);
        } else {
            logger.info("Checking for required tech packs skipped.");
            responseData.setMessage("Checking for required tech packs skipped.");
            return new ResponseEntity<>(responseData, HttpStatus.OK);
        }
    }
}
