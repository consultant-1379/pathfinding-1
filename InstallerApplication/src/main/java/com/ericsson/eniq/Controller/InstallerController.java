package com.ericsson.eniq.Controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import com.ericsson.eniq.Services.ActivateInterface;
import com.ericsson.eniq.Services.BackupAndRestore;
import com.ericsson.eniq.Services.DWHMStorageTimeAction;
import com.ericsson.eniq.Services.ETLSetImport;
import com.ericsson.eniq.Services.GetInputData;
import com.ericsson.eniq.Services.HandleBusyhourActivation;
import com.ericsson.eniq.Services.InputForm;
import com.ericsson.eniq.Services.MetaInstallation;
import com.ericsson.eniq.Services.PreInstallCheck;
import com.ericsson.eniq.Services.TPInstallOrderer;
import com.ericsson.eniq.Services.TechPackAndTypeActivation;
import com.ericsson.eniq.Services.UpdateDataItem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


@Controller
public class InstallerController {

	private static final Logger logger = LogManager.getLogger(InstallerController.class);
	@RequestMapping (value="/testinstall" , method =RequestMethod.GET)
	@ResponseBody
	public String testInstall(){
		
		return "It is installer software";
	}
	
	
	@RequestMapping (value="/install" , method = RequestMethod.POST, consumes="application/json")
	@ResponseBody
	public void install(@RequestBody GetInputData inputList) throws Exception  {
		String tpDirectory="";
		String tpName="";
		String stageFile="";
		String ossId="";
		String importingInterface="false";
		boolean stageDone=false;
		List<InputForm> list = inputList.getInputList();
	    if (list != null) {
		  for(InputForm inputData : list) {
			  tpDirectory=inputData.getTpDirectory();
			  tpName=inputData.getTpName();
			  stageFile=inputData.getStageFile();
			  ossId=inputData.getOssId();
		  } 
		  
		
		  
		 	  
		 stageDone=getStageList(stageFile,"TPInstallOrderer");
		      if(!stageDone)
		      {
		    	  TPInstallOrderer tpo=new TPInstallOrderer(tpName,tpDirectory); 
				  int result=tpo.execute(); 
				  if(result==0)
				  {
					  System.exit(0);
				  }
				  writeStageFile(stageFile,"TPInstallOrderer");
		      }
		      else
		      {
		    	  logger.info("Stage TPInstallOrderer already completed");
		      }
		  stageDone=getStageList(stageFile,"PreInstallCheck");
			  if(!stageDone)
			  {
				  PreInstallCheck pc=new PreInstallCheck(tpName,tpDirectory); 
				  pc.execute();
				  writeStageFile(stageFile,"PreInstallCheck");
			  }
			  else
		      {
		    	  logger.info("Stage PreInstallCheck already completed");
		      }
		      
		      stageDone=getStageList(stageFile,"BackUp");
		  	  if(!stageDone)
	          {
			  BackupAndRestore bnr=new BackupAndRestore();
			  bnr.backup();
			  writeStageFile(stageFile,"BackUp");
	          }
	      
			  stageDone=getStageList(stageFile,"MetaInstallation");
			  if(!stageDone)
			  {
				  MetaInstallation mi=new MetaInstallation(tpName,tpDirectory);
				  mi.execute(); 
				  writeStageFile(stageFile,"MetaInstallation");
			  }
			  else
		      {
		    	  logger.info("Stage MetaInstallation already completed");
		      }
			  stageDone=getStageList(stageFile,"UpdateDataItem");
			  if(!stageDone)
			  {
				  UpdateDataItem udi=new UpdateDataItem(tpDirectory);	
				  udi.execute();
				  writeStageFile(stageFile,"UpdateDataItem");
			  }
			  else
		      {
		    	  logger.info("Stage UpdateDataItem already completed");
		      }
			  stageDone=getStageList(stageFile,"ETLSetImport");
			  if(!stageDone)
			  {
				  if(tpName.startsWith("INTF"))
				  {
					  importingInterface="true";
					  ETLSetImport esi=new ETLSetImport(tpName,tpDirectory,importingInterface);
					  esi.execute();
				  }
				  else
				  {
					  ETLSetImport esi=new ETLSetImport(tpName,tpDirectory,importingInterface);
					  esi.execute();
					  
				  }
				  writeStageFile(stageFile,"ETLSetImport");
			  }
			  else
		      {
		    	  logger.info("Stage ETLSetImport already completed");
		      }
			  
			  stageDone=getStageList(stageFile,"HandleBusyhourActivation");
			  if(!stageDone)
			  {
				  HandleBusyhourActivation hba=new HandleBusyhourActivation(tpDirectory,tpName);	
				  hba.execute();
				  writeStageFile(stageFile,"HandleBusyhourActivation");
			  }
			  else
		      {
		    	  logger.info("Stage HandleBusyhourActivation already completed");
		      }
			  stageDone=getStageList(stageFile,"TechPackAndTypeActivation");
			  if(!stageDone)
			  {
				  TechPackAndTypeActivation ta=new TechPackAndTypeActivation(tpDirectory,tpName);	
				  ta.execute();
				  writeStageFile(stageFile,"TechPackAndTypeActivation");
			  }
			  else
		      {
		    	  logger.info("Stage TechPackAndTypeActivation already completed");
		      }	
			  
			  stageDone=getStageList(stageFile,"DWHMStorageTimeAction");
			  if(!stageDone)
			  {
				  DWHMStorageTimeAction dwhms=new DWHMStorageTimeAction(tpName,tpDirectory);
				  dwhms.execute();
				  writeStageFile(stageFile,"DWHMStorageTimeAction");
			  }
			  else
		      {
		    	  logger.info("Stage DWHMStorageTimeAction already completed");
		      }	
			  
		     
	    }
	    
	}
	
	@RequestMapping (value="/activate" , method = RequestMethod.POST, consumes="application/json")
	@ResponseBody
	public void activate(@RequestBody GetInputData inputList) throws Exception  {
		String ossId="";
		String tpName="";
		List<InputForm> list = inputList.getInputList();
	    if (list != null) {
		  for(InputForm inputData : list) {
			  tpName=inputData.getTpName();
			  ossId=inputData.getOssId();
		  }  
		  ActivateInterface ai=new ActivateInterface(tpName,ossId);
		  ai.execute();
	    }
		  
	}
	
	private boolean getStageList(final String stageFile,final String stageName) throws Exception
	{
		File f=new File(stageFile);
		boolean flag=false;
		if(!f.exists())
		{
			return false;
		}
		try(FileReader reader = new FileReader(f);BufferedReader bufferedReader = new BufferedReader(reader))
		{
			 String line;
	         while ((line = bufferedReader.readLine()) != null) {
	                if(line.equalsIgnoreCase(stageName))
	                {
	                	flag=true;
	                	break;
	                }
	         }
		}
		catch(Exception e)
		{
			logger.info("Exception in reading stage file");
			e.printStackTrace();
			throw new Exception("Error reading stage list file", e);
		}
		if(flag)
		{
			return true;
		}
		return false;
	}
	
	private void writeStageFile(final String stageFile,final String stageName) throws Exception
	{
		File f=new File(stageFile);
		if(!f.exists())
		{
			f.createNewFile();
		}
		try(FileWriter writer = new FileWriter(f,true);BufferedWriter bufferedWriter = new BufferedWriter(writer))
		{
			bufferedWriter.append(stageName);
			bufferedWriter.newLine();
		}
		catch(IOException e)
		{
			logger.info("Exception in writing stage file");
			e.printStackTrace();
			throw new Exception("Error writing stage list file", e);
		}
	}
}
