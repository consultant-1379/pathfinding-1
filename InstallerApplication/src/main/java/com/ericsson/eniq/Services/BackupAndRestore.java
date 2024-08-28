package com.ericsson.eniq.Services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

public class BackupAndRestore {
	
	private static final Logger logger = LogManager.getLogger(BackupAndRestore.class);

	RestTemplate restTemplate=new RestTemplate();
	String backupAndRestoreURL="http://10.108.213.238:7001/v1/backup-manager/configuration-data/action";
	String backupJsonString="{\"action\": \"CREATE_BACKUP\",\"payload\": {\"backupName\": \"myBackup\"}}";
	String restoreJsonString="{\"action\": \"RESTORE\",\"payload\": {\"backupName\": \"myBackup\"}}";
	
	public void backup()
	{
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		HttpEntity<String> entity = new HttpEntity<String>(backupJsonString,headers);
		String backupId = restTemplate.postForObject(backupAndRestoreURL, entity, String.class);
		logger.info("Backup completed :"+backupId);
		
		String backupoutput= restTemplate.postForObject("http://10.108.213.238:7001/v1/backup-manager/configuration-data/backup/myBackup", entity, String.class);
	    while(!backupoutput.contains("\"status\":\"COMPLETE\""))
	    {
	    	backupoutput= restTemplate.postForObject("http://10.108.213.238:7001/v1/backup-manager/configuration-data/backup/myBackup", entity, String.class);
	    }
	}
	
	
	public void restore()
	{
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);

		HttpEntity<String> entity = new HttpEntity<String>(restoreJsonString,headers);
		String restoreId = restTemplate.postForObject(backupAndRestoreURL, entity, String.class);
		logger.info("Restore completed :"+restoreId);
	}
}
