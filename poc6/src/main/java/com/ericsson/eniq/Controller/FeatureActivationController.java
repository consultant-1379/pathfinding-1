package com.ericsson.eniq.Controller;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.ericsson.eniq.bean.FeatureActivationRequest;
import com.ericsson.eniq.model.FeatureActivation;
import com.ericsson.eniq.service.FeatureActivationService;


@Controller
public class FeatureActivationController {
	
	private static final Logger logger = LoggerFactory.getLogger(FeatureActivationController.class);
	
	
//	@Autowired
//	private FeatureActivationRepository featureActivationRepository ;
//	
//	@Autowired
//	private TechpackActivationRepository techpackActivationRepository;
	
	@Autowired
	private FeatureActivationService activationService;
	
	private final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	

	@RequestMapping(value = "/getFeatureActivation", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<FeatureActivation> getFeatureActivation(Model model,
			@RequestParam(required = true) String featureDesc) {

		//FeatureActivation featureActivation = new FeatureActivation();
		// prepareLicenseResponse
		FeatureActivation featureActivation = activationService.getFeatureActivation(featureDesc);
		logger.info("License response object" + featureActivation);
		return ResponseEntity.ok(featureActivation);
	}
	
	@RequestMapping(value = "/updateFeatureActivation", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<FeatureActivation> updateFeatureActivation(Model model,
			@RequestBody(required = true) FeatureActivationRequest featureActRequest) {

		//FeatureActivation featureActivation = new FeatureActivation();
		// prepareLicenseResponse
		FeatureActivation featureActivation = activationService.updateFeatureActivation(featureActRequest);
		logger.info("License response object" + featureActivation);
		return ResponseEntity.ok(featureActivation);
	}
	
	
	
	@ExceptionHandler(Exception.class)
	@RequestMapping(value = "/intallFeature", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity intallFeature(Model model,
			@RequestBody(required = true) FeatureActivationRequest activationRequest) throws Exception {
		
		String status = "";
		FeatureActivation featureActivation = new FeatureActivation();
		try {
			featureActivation = activationService.intallFeature(activationRequest);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return ResponseEntity
			        .status(HttpStatus.INTERNAL_SERVER_ERROR)
			        .body(e.getMessage());
		}
		//featureActivationRepository.save(featureActivation);
		status = "Feature Installed and Activated";
		logger.info("Installed Feature object" + featureActivation.toString());
		return ResponseEntity.ok(featureActivation);
	}
	
	
	private java.sql.Timestamp parseTimestamp(String timestamp) {
	
	    try {
	        return new Timestamp(DATE_TIME_FORMAT.parse(timestamp).getTime());
	    } catch (ParseException e) {
	        throw new IllegalArgumentException(e);
	    }
	}

}
