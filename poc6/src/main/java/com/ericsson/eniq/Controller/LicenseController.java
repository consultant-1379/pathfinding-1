package com.ericsson.eniq.Controller;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.ericsson.eniq.bean.GetLicense;
import com.ericsson.eniq.bean.GetLicenseResponse;
import com.ericsson.eniq.bean.LicenseInfo;

@Controller
public class LicenseController {

	private static final Logger logger = LoggerFactory.getLogger(LicenseController.class);

	@Autowired
    private GetLicenseResponse getLicenseResponse ;

	@RequestMapping(value = "/getlicense", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<GetLicenseResponse> getLicense(Model model,
			@RequestBody(required = true) GetLicense getLicense) {
		
		//getLicenseResponse.getLicensesInfo().ge
		
		GetLicenseResponse getLicenseResponse1 = new GetLicenseResponse();
		
		getLicenseResponse1.setOperationalStatusInfo(getLicenseResponse.getOperationalStatusInfo());
		getLicenseResponse1.setLicensesInfo(getLicenseResponse.getLicensesInfo());

		// prepareLicenseResponse
		logger.info("License response object" + getLicenseResponse1.toString());
		return ResponseEntity.ok(getLicenseResponse1);
	}

	@RequestMapping(value = "/getlicensestatus", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> licenseStatus(Model model, @RequestParam(required = true) String licenseId) {

		String licenseStatus = "License Not Found";

		// prepareLicenseResponse
		// GetLicenseResponse getLicenseResponse = licenseUtil.prepareLicenseResponse();
		List<LicenseInfo> licenseInfos = getLicenseResponse.getLicensesInfo();
		System.out.println("licenseId...." + licenseId);
		for (Iterator<LicenseInfo> iterator = licenseInfos.iterator(); iterator.hasNext();) {
			LicenseInfo licenseInfo = iterator.next();

			if (licenseInfo.getLicense().getKeyId().equals(licenseId)) {
				licenseStatus = "License Status is " + licenseInfo.getLicenseStatus();
			}
		}

		logger.info("License Status " + licenseStatus);
		return ResponseEntity.ok(licenseStatus);
	}

}
