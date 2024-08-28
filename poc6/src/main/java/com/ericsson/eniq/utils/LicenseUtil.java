////package com.ericsson.eniq.utils;
////
////import java.util.ArrayList;
////import java.util.List;
////
////import org.springframework.beans.factory.annotation.Autowired;
////import org.springframework.stereotype.Component;
////
////import com.ericsson.eniq.bean.CapacityData;
////import com.ericsson.eniq.bean.GetLicenseResponse;
////import com.ericsson.eniq.bean.License;
////import com.ericsson.eniq.bean.LicenseInfo;
////import com.ericsson.eniq.bean.LicenseStatus;
////import com.ericsson.eniq.bean.LicenseType;
////import com.ericsson.eniq.bean.OperationalMode;
////import com.ericsson.eniq.bean.OperationalStatus;
////
////@Component
////public class LicenseUtil {
////	
////	@Autowired
////	private GetLicenseResponse getLicenseResponse;
////
////	public GetLicenseResponse prepareLicenseResponse() {
////
////		//GetLicenseResponse licenseResponse = new GetLicenseResponse();
////		OperationalStatus operationalStatusInfo = new OperationalStatus();
////		operationalStatusInfo.setAutonomousModeDuration(10);
////		operationalStatusInfo.setOperationalMode(OperationalMode.NORMAL);
////		licenseResponse.setOperationalStatusInfo(operationalStatusInfo);
////		
////		List<LicenseInfo> licensesInfo = new ArrayList<LicenseInfo>();
////		
//		LicenseInfo licenseInfo = new LicenseInfo();
//		CapacityData capacityInfo = new CapacityData();
//		capacityInfo.setIsLimited(true);
//		capacityInfo.setLicensedCapacity(10);
//		licenseInfo.setCapacityInfo(capacityInfo);
//		
//		License license = new License();
//		license.setKeyId("U0JKDDL");
//		license.setType(LicenseType.CAPACITY_CUMULATIVE);
//		licenseInfo.setLicense(license);
//		
//		licenseInfo.setLicenseStatus(LicenseStatus.VALID);
//		
//		licensesInfo.add(licenseInfo);
//		
//		
//		licenseResponse.setLicensesInfo(licensesInfo);
//		return licenseResponse;
//	}
//
//}
