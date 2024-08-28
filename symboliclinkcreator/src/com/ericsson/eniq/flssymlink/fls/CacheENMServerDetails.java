package com.ericsson.eniq.flssymlink.fls;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.eniq.flssymlink.StaticProperties;

/**
 * This class compares two files and creates a cache of the ENM server details
 * in the cache memory.
 *  
 */
class CacheENMServerDetails {

	private static Map<String, ENMServerDetails> cache = new HashMap<>();
	private static final String OSS_REF_NAME_FILE = "/eniq/sw/conf/.oss_ref_name_file";
	private static final String ENM_FILE_PATH = "/eniq/sw/conf/enmserverdetail";
	
	private CacheENMServerDetails() {
		
	}
	
	
	
	static void init(Logger log){
		Map<String, String> ossRefMap = new HashMap<>();
		String ossRefFile = StaticProperties.getProperty("OSS_REF_NAME_FILE_PATH", OSS_REF_NAME_FILE);
		String enmServerDetailFile = StaticProperties.getProperty("ENM_FILE_PATH", ENM_FILE_PATH);
		try (BufferedReader inOssRef = new BufferedReader(new FileReader(new File(ossRefFile)));
				BufferedReader inEnmDetail = new BufferedReader(new FileReader(new File(enmServerDetailFile)));) {
			String line;
			while ((line = inOssRef.readLine()) != null) {
				String[] oss = line.split("\\s+");
				ossRefMap.put(oss[1], oss[0]);
			}
			String line1;
			while ((line1 = inEnmDetail.readLine()) != null) {
				String[] enm = line1.split("\\s+");
				String ip = enm[0];
				String ossId = ossRefMap.get(ip);
				if (ossId != null) {
					ENMServerDetails element = new ENMServerDetails();
					element.setIp(enm[0]);
					element.setHost(enm[1]);
					element.setType(enm[2]);
					element.setUsername(enm[3]);
					element.setPassword(enm[4]);
					element.setHostname(enm[5]);
					cache.put(ossId, element);
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING,"Error while building ENMServerDetails Cache",e);
		} 
		if (cache.isEmpty()) {
			log.log(Level.INFO, "ENMServerDetails Cache is empty");
		}
		log.log(Level.INFO, "Built ENMServerDetails cache = "+cache);
	}
	
	static Map<String, ENMServerDetails> getCache() {
		return cache;
	}
	
}

