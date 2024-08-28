package com.ericsson.oss.eniq.techpack.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ericsson.oss.eniq.techpack.CoreApplication;


@RestController
@RequestMapping
public class FeatureApplicationController {
	  Logger logger = LoggerFactory.getLogger(CoreApplication.class);
	  
	  private static final String STATUS = "Feature Application Deployed";
	  private static final String LOGS = "SAMPLE_LOGS_LEVELS_LOGGED";


	  @GetMapping("featureManager/status")
	    public String featureManagerRunningStatus() {
	        logger.info("Feature Manager status "+ STATUS);
	        return STATUS;
	    }

	    @GetMapping("featureManager/checklogs")
	    public String loggingApplicationCheckLogs() {
	        logger.info("Feature Manager info");
	        logger.warn("Feature Manager warn");
	        logger.error("Feature Manager error");
	        logger.debug("Feature Manager debug");
	        return LOGS;
	    }

}
