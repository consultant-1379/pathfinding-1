package com.ericsson.eniq.teckpack.createTP;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTPUtils {

	public static String evaluteCountAggreation(Map<String, Object> measType, Map<String, Object> deltacalcSupport,
			Map<String, Object> dataStore) {
		Map<String, Object> versioning = (Map<String, Object>) dataStore.get("Versioning");
		if ((Integer) measType.get("DELTACALCSUPPORT") == 1) {
			List<String> sVR = (List<String>) versioning.get("SupportedVendorRelease");
			List<String> countAggSupportedList = new ArrayList();
			List<String> countAggNotSupportedList = new ArrayList();
			String flagTreatAs = null;
			if (deltacalcSupport.containsKey("VENDORRELEASE")) {
				String deltaCalc = (String) deltacalcSupport.get("VENDORRELEASE");
				if (deltaCalc.contains(":")) {
					int index = deltaCalc.indexOf(":");
					flagTreatAs = deltaCalc.substring(index);
					deltaCalc = deltaCalc.substring(0, index);
				}
				String supportedReleases = deltaCalc;
				for (String VendorRelease : sVR) {
					if (VendorRelease.contains(supportedReleases)) {
						countAggSupportedList.add(VendorRelease);
					} else {
						countAggNotSupportedList.add(VendorRelease);
					}
				}
			}
			if (countAggSupportedList.isEmpty()) {
				return "GAUGE";
			} else if (countAggNotSupportedList.isEmpty()) {
				return "PEG";
			} else {
				String countAggSupp = String.join(",", countAggSupportedList) + ";PEG/"
						+ String.join(",", countAggNotSupportedList) + ";GAUGE";
				if (flagTreatAs != null) {
					countAggSupp = countAggSupp + flagTreatAs;
				}

				return countAggSupp;
			}
		}
		return "";
	}

}
