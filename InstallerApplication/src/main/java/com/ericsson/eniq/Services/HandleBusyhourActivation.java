package com.ericsson.eniq.Services;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;


import com.distocraft.dc5000.repository.dwhrep.Aggregationrule;
import com.distocraft.dc5000.repository.dwhrep.AggregationruleFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhour;
import com.distocraft.dc5000.repository.dwhrep.BusyhourFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhourmapping;
import com.distocraft.dc5000.repository.dwhrep.BusyhourmappingFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhourplaceholders;
import com.distocraft.dc5000.repository.dwhrep.BusyhourplaceholdersFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhourrankkeys;
import com.distocraft.dc5000.repository.dwhrep.BusyhourrankkeysFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhoursource;
import com.distocraft.dc5000.repository.dwhrep.BusyhoursourceFactory;
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;

@Component
public class HandleBusyhourActivation {
	
	private static final Logger logger = LogManager.getLogger(HandleBusyhourActivation.class);

	private transient RockFactory etlrepRockFactory = null;
	private transient RockFactory dwhrepRockFactory = null;
	private transient String propFilepath = "";
	private String techPackVersionID;

	// parameters from ANT
	private String configurationDirectory = "";
	private String techPackName = "";
	private int techPackMetadataVersion = 0;
	private int buildNumber = 0;
	private String techPackVersion = "";
	private String tpDirectory="";
	private String tpName="";
	private final Properties props = new Properties();
	public HandleBusyhourActivation()
	{
		
	}
	public HandleBusyhourActivation(final String tpDirectory,final String tpName)
	{
		this.tpDirectory=tpDirectory;
		this.tpName=tpName;
	}
	
	private void readTechPackVersionFile() throws Exception{

		final String targetFilePath = this.tpDirectory+"/"+this.tpName + "/install/version.properties";
		final File targetFile = new File(targetFilePath);
		if (!targetFile.isFile() || !targetFile.canRead()) {
			throw new Exception("Could not read file " + targetFilePath + ". Please check that the file "
					+ targetFilePath + " exists and it can be read.");
		} else {
			logger.info("Reading file: " + targetFilePath);
		}

		try {

			props.load(new FileInputStream(targetFile));
			this.techPackName = props.getProperty("tech_pack.name");
			this.techPackVersion = props.getProperty("tech_pack.version");
			this.buildNumber = Integer.parseInt(props.getProperty("build.number"));
			this.techPackMetadataVersion = Integer.parseInt(props.getProperty("tech_pack.metadata_version"));
		}
		catch(Exception e)
		{
			logger.info("Exception in reading version.properties file");
		}
	}

	public void execute() throws Exception {
		readTechPackVersionFile();

		if (techPackMetadataVersion >= 3) {
			techPackVersionID = this.techPackName + ":((" + this.buildNumber
					+ "))";
		} else if (techPackMetadataVersion == 2) {
			techPackVersionID = this.techPackName + ":b" + this.buildNumber;
		} else {
			techPackVersionID = this.techPackName + ":" + this.techPackVersion
					+ "_b" + this.buildNumber;
		}

		logger.info("Checking connection to database");
		GetDatabaseDetails getdb=new GetDatabaseDetails();
		
		final Map<String, String> databaseConnectionDetails =getdb.getDatabaseConnectionDetails();
		
		
		this.etlrepRockFactory = getdb.createEtlrepRockFactory(databaseConnectionDetails);

		
		this.dwhrepRockFactory=getdb.createDwhrepRockFactory(this.etlrepRockFactory);

		logger.info("Connections to database created.");
		

		if(checkBusyhourPlaceholdersExist()){
			updateBusyhourEnableFlags();
			updateBusyhourPlaceholders();
			updateAggregationRules();
			copyCustomBusyhours();
			copyCustomBusyhourmapping();
			copyCustomBusyhourRankkeys();
			copyCustomBusyhourSource();
		}
	}

	

	

	/**
	 * This function returns a previous version of techpack activation if it
	 * exists in table TPActivation. If it doesn't exist, null is returned.
	 * 
	 * @param techPackName
	 *            is the name of the techpack to search for.
	 * @return Returns Tpactivation instace if a previous version of
	 *         TPActivation exists, otherwise returns null.
	 */
	private Tpactivation getPredecessorTPActivation(final String techPackName)
			throws Exception {

		Tpactivation targetTPActivation = null;

		try {
			final Tpactivation whereTPActivation = new Tpactivation(
					this.dwhrepRockFactory);
			whereTPActivation.setTechpack_name(techPackName);
			final TpactivationFactory tpActivationFactory = new TpactivationFactory(
					this.dwhrepRockFactory, whereTPActivation);

			final Vector<Tpactivation> tpActivations = tpActivationFactory
					.get();
			if (tpActivations.size() > 0) {
				targetTPActivation = tpActivations.get(0);
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(
					"Getting predecessor TPActivation failed.", e);
		}

		return targetTPActivation;
	}

	/**
	 * This method performs a check to see if there are Placeholders 
	 * from the "old TP". 
	 * If there is, then the "old TP" contains both 
	 * PPs and CPs and its data needs to be copied to the "new TP".
	 * If there isn't, then the "old TP" doesn't contain any information 
	 * which needs to be copied over to "new TP".
	 * @return TRUE (if Placeholders exist), FALSE otherwise.
	 */
	private boolean checkBusyhourPlaceholdersExist(){
		boolean result = false;
		String prevversionid = "";
		try {
			logger.info("Checking the presence of BH Placeholders in Active TechPack...");

			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);
			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null){
				// fetch the previous (old) TP BusyhourPlaceholders
				prevversionid = preTPActivation.getVersionid();
				final Busyhourplaceholders oldBHPlaceholdersSearch = new Busyhourplaceholders(this.dwhrepRockFactory);
				oldBHPlaceholdersSearch.setVersionid(prevversionid);
				BusyhourplaceholdersFactory oldBHPlaceholdersFactory = new BusyhourplaceholdersFactory(
						this.dwhrepRockFactory, oldBHPlaceholdersSearch);
				if(oldBHPlaceholdersFactory.size() > 0){
					result = true;	
				}
			}
		} catch (SQLException e) {
			logger.info("Failed to check the presence of BH Placeholders in Active TechPack...");
			e.printStackTrace();
		} catch (RockException e) {
			logger.info("Failed to check the presence of BH Placeholders in Active TechPack...");
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(result){
			logger.info("...Active TP: "+prevversionid+" has placeholders. ");
		}else{
			logger.info("...Active TP: "+prevversionid+" has no placeholders. There are no Product or Custom Placeholders to copy to new TP:"+techPackVersionID);
		}
		return result;
	}
	
	
	/**
	 * Updates the busyhours enable flags to match the previously activated TP
	 * @return
	 * @throws Exception 
	 */
	private int updateBusyhourEnableFlags() throws Exception {
		int updatedBusyhourStatuses = 0;

		try {

			
			logger.info("Checking Busyhour Product Placeholder enabled states...");
			
			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);

			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null) {

				

				
				// fetch the previous (old) techpacks product busyhours
				String prevversionid = preTPActivation.getVersionid();
				final Busyhour prevbh = new Busyhour(this.dwhrepRockFactory);
				prevbh.setVersionid(prevversionid);
				prevbh.setPlaceholdertype("PP");
				BusyhourFactory prevbhF = new BusyhourFactory(
						this.dwhrepRockFactory, prevbh);

				// fetch the installed (new) techpacks product busyhours
				final Busyhour installedbh = new Busyhour(
						this.dwhrepRockFactory);
				installedbh.setVersionid(techPackVersionID);
				installedbh.setPlaceholdertype("PP");
				BusyhourFactory installedbhF = new BusyhourFactory(
						this.dwhrepRockFactory, installedbh);


				
				// loop all new TP busyhours match them with old TP busyhours
				for (Busyhour oldBH : prevbhF.get()) {

					for (Busyhour newBH : installedbhF.get()) {

						// if we found a match and the new TP bhcriteria is not empty we change the new TP busyhour
						// status to old TP busyhour status
						if (oldBH.getBhlevel().equals(newBH.getBhlevel())
								&& oldBH.getBhtype().equals(newBH.getBhtype())
								&& !oldBH.getBhcriteria().equals("")) {

							newBH.setEnable(oldBH.getEnable());
							newBH.saveToDB();
							updatedBusyhourStatuses++;
						}
					}
				}
			} else {
				logger.info("No previous techpack installed, no need to update busyhour enabled states.");
			}
			
			logger.info("Maintaining "+updatedBusyhourStatuses+" Busyhour Product Placeholder enabled states.");
			
		} catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Maintaining Busyhour Product Placeholder enabled states failed.", e);
		}
		return updatedBusyhourStatuses;
	}

	/**
	 * Updates both the Product and Custom AggregationRules.
	 * @return
	 * @throws Exception 
	 */
	private int updateAggregationRules() throws Exception {
		int updatedBusyhourStatuses = 0;

		try {
			logger.info("Updating AggregationRules...");
			
			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);

			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null) {
				// fetch the previous (old) techpacks Aggregationrule (PP & CP)
				String prevversionid = preTPActivation.getVersionid();
				//Update the Product Busyhour AggergationRules.
				updateProductBHAggregationRules(prevversionid);
				//Update the Custom Busyhour AggregationRules.
				copyCustomBHAggregationRules(prevversionid);
			} else {
				logger.info("No previous tecpack installed, no need to update AggregationRules.");
			}
		} catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Updating AggregationRules failed.", e);
		}
		return updatedBusyhourStatuses;
	}

	/**
	 * This  method is required to change the ENABLED status of the AggregationRules that
	 * existed on the old TP. This method will not change the ENABLED status of new 
	 * Product Busyhours delivered in the new TP. 
	 * This method must first find all old PP's that have a source_type (i.e. it's not an empty String).
	 * This means that these PP's existed in the old TP. The ENABLED status of the old PP's must be 
	 * updated to the new TP as the user could enable/disable a PP in the AdminUI, thus enabling/disabling 
	 * the linked AggregationRule (DAY|WEEK|MONTH).  
	 * @param oldVersionId
	 * @return
	 * @throws Exception 
	 */
	private int updateProductBHAggregationRules(String oldVersionId) throws Exception{
		int updatedPBHAggregationRules = 0;
		try {
			logger.info("Updating AggregationRules for Product Busyhours...");

			//Get all AggregationRules from oldTP
			final Aggregationrule oldAggregationRuleSearch = new Aggregationrule(this.dwhrepRockFactory);
			oldAggregationRuleSearch.setVersionid(oldVersionId);
			oldAggregationRuleSearch.setTarget_level("RANKBH");
			oldAggregationRuleSearch.setAggregationscope("DAY");
			AggregationruleFactory oldAggregationRuleFactory = new AggregationruleFactory(
					this.dwhrepRockFactory, oldAggregationRuleSearch);

			for(Aggregationrule oldAggregationRule : oldAggregationRuleFactory.get()){
				String bhtype = oldAggregationRule.getBhtype();
				if(bhtype.startsWith("PP", bhtype.lastIndexOf("_")+1)){
					if(oldAggregationRule.getSource_type().equals("")){
						//Do nothing. This PP doesn't exist on the old TP.
					}else{
						//Copy the enabled state to the DAY, WEEK and MONTH
						//get the DAY, WEEK, MONTH AggregationRule...
						final Aggregationrule newAggregationRuleSearch = new Aggregationrule(this.dwhrepRockFactory);
						newAggregationRuleSearch.setVersionid(techPackVersionID);
						newAggregationRuleSearch.setTarget_level("RANKBH");
						newAggregationRuleSearch.setTarget_type(oldAggregationRule.getTarget_type());
						newAggregationRuleSearch.setBhtype(oldAggregationRule.getBhtype());
						AggregationruleFactory newAggregationRuleFactory = new AggregationruleFactory(
								this.dwhrepRockFactory, newAggregationRuleSearch);
						for(Aggregationrule newAggregationRule:newAggregationRuleFactory.get()){
							newAggregationRule.setEnable(oldAggregationRule.getEnable());
							newAggregationRule.saveToDB();
							updatedPBHAggregationRules++;
							logger.info("Updated enabled state of: "+newAggregationRule.getAggregation());
						}
					}
				}
				
			}
			logger.info("Updated "+updatedPBHAggregationRules+" Product Busyhour AggregationRules.");

		}catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Updating AggregationRules (Product Busyhours) enabled flags failed.", e);
		}

		return updatedPBHAggregationRules;
	}
	
	/**
	 * 
	 * @param oldVersionId
	 * @return
	 * @throws Exception 
	 */
	private int copyCustomBHAggregationRules(String oldVersionId) throws Exception{
		int updatedCBHAggregationRules = 0;
		//Add the name of the banned setters in lowercase!
		ArrayList<String> bannedSetters = new ArrayList<String>();
		bannedSetters.add("setversionid");
		bannedSetters.add("settarget_mtableid");
		bannedSetters.add("setsource_mtableid");

		try {
			logger.info("Updating AggregationRules for Custom Busyhours...");

			//Get all AggregationRules from oldTP
			final Aggregationrule oldDAYAggregationRuleSearch = new Aggregationrule(this.dwhrepRockFactory);
			oldDAYAggregationRuleSearch.setVersionid(oldVersionId);
			oldDAYAggregationRuleSearch.setTarget_level("RANKBH");
			oldDAYAggregationRuleSearch.setAggregationscope("DAY");
			AggregationruleFactory oldDAYAggregationRuleFactory = new AggregationruleFactory(
					this.dwhrepRockFactory, oldDAYAggregationRuleSearch);

			for(Aggregationrule oldDAYAggregationRule: oldDAYAggregationRuleFactory.get()){
				String bhtype = oldDAYAggregationRule.getBhtype();
				if(bhtype.startsWith("CP", bhtype.lastIndexOf("_")+1)){
					if(oldDAYAggregationRule.getSource_type().equals("")){
						//Do nothing. This CP doesn't exist on the old TP.
					}else{
						//Copy AggregationRules to  DAY, WEEK and MONTH
						//now need to get all the CP's from oldTP and newTP that match this criteria!
						//AggregationRules from old TP.
						final Aggregationrule oldAggregationRuleSearch = new Aggregationrule(this.dwhrepRockFactory);
						oldAggregationRuleSearch.setVersionid(oldVersionId);
						oldAggregationRuleSearch.setTarget_level("RANKBH");
						oldAggregationRuleSearch.setTarget_type(oldDAYAggregationRule.getTarget_type());
						oldAggregationRuleSearch.setBhtype(oldDAYAggregationRule.getBhtype());
						AggregationruleFactory oldAggregationRuleFactory = new AggregationruleFactory(
								this.dwhrepRockFactory, oldAggregationRuleSearch);

						//AggregationRules from new TP.
						final Aggregationrule newAggregationRuleSearch = new Aggregationrule(this.dwhrepRockFactory);
						newAggregationRuleSearch.setVersionid(techPackVersionID);
						newAggregationRuleSearch.setTarget_level("RANKBH");
						newAggregationRuleSearch.setTarget_type(oldDAYAggregationRule.getTarget_type());
						newAggregationRuleSearch.setBhtype(oldDAYAggregationRule.getBhtype());
						AggregationruleFactory newAggregationRuleFactory = new AggregationruleFactory(
								this.dwhrepRockFactory, newAggregationRuleSearch);

						for (Aggregationrule oldAggregationRule : oldAggregationRuleFactory.get()) {
							for (Aggregationrule newAggregationRule : newAggregationRuleFactory.get()) {
								if(oldAggregationRule.getAggregation().equals(newAggregationRule.getAggregation()) 
										&& oldAggregationRule.getRuleid().equals(newAggregationRule.getRuleid())){
									copy(oldAggregationRule, newAggregationRule, bannedSetters);
									String mtableid = oldAggregationRule.getSource_mtableid().replace(oldVersionId, techPackVersionID);
									newAggregationRule.setSource_mtableid(mtableid);
									newAggregationRule.saveToDB();
									updatedCBHAggregationRules++;
									logger.info("Copied: "+newAggregationRule.getAggregation());
								}
							}
						}
					}					
				}
			}
			logger.info("Copied "+updatedCBHAggregationRules+" Custom Busyhour AggregationRules.");
		}catch(Exception e){
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Copying AggregationRules (Custom Busyhours) failed.", e);
		}
		return updatedCBHAggregationRules;
	}
	/**
	 * Updates the number of CP Busyhour Placeholders in new TP to match the old TP.
	 * @return
	 * @throws Exception 
	 */
	private int updateBusyhourPlaceholders() throws Exception{
		int updatedBusyhourPlaceholders = 0;

		try {

			
			logger.info("Updating Busyhour Placeholders...");
			
			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);

			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null) {
				
				// fetch the previous (old) TP BusyhourPlaceholders
				String prevversionid = preTPActivation.getVersionid();
				final Busyhourplaceholders oldBHPlaceholdersSearch = new Busyhourplaceholders(this.dwhrepRockFactory);
				oldBHPlaceholdersSearch.setVersionid(prevversionid);
				BusyhourplaceholdersFactory oldBHPlaceholdersFactory = new BusyhourplaceholdersFactory(
						this.dwhrepRockFactory, oldBHPlaceholdersSearch);

				// fetch the installed (new) TP BusyhourPlaceholders
				final Busyhourplaceholders newBHPlaceholdersSearch = new Busyhourplaceholders(
						this.dwhrepRockFactory);
				newBHPlaceholdersSearch.setVersionid(techPackVersionID);
				BusyhourplaceholdersFactory newBHPlaceholdersFactory = new BusyhourplaceholdersFactory(
						this.dwhrepRockFactory, newBHPlaceholdersSearch);


				
				// loop all new TP busyhours match them with old TP busyhours
				for (Busyhourplaceholders oldBHPlaceholders : oldBHPlaceholdersFactory.get()) {

					for (Busyhourplaceholders newBHPlaceholders : newBHPlaceholdersFactory.get()) {
						
						if (oldBHPlaceholders.getBhlevel().equals(newBHPlaceholders.getBhlevel())) {
							newBHPlaceholders.setCustomplaceholders(oldBHPlaceholders.getCustomplaceholders());
							newBHPlaceholders.saveToDB();
							updatedBusyhourPlaceholders++;
						}
					}
				}
			} else {
				logger.info("No previous tecpack installed, no need to update Busyhour Placeholders.");
			}
			logger.info("Updated "+updatedBusyhourPlaceholders+" Busyhour Placeholders.");
		} catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Updating Busyhour Placeholders failed.", e);
		}
		return updatedBusyhourPlaceholders;
		
	}
	
	/**
	 * Copies the Custom Busyhours from the old TP into the new TP.
	 * @return
	 * @throws Exception 
	 */
	private int copyCustomBusyhours() throws Exception {
		int updatedCustomBusyhours = 0;

		//Add the name of the banned setters in lowercase!
		ArrayList<String> bannedSetters = new ArrayList<String>();
		bannedSetters.add("setversionid");
		bannedSetters.add("settargetversionid");

		try {
			logger.info("Copying Custom Busyhours...");
			
			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);

			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null) {

				// fetch the previous (old) techpacks product busyhours
				String prevversionid = preTPActivation.getVersionid();
				final Busyhour prevbh = new Busyhour(this.dwhrepRockFactory);
				prevbh.setVersionid(prevversionid);
				prevbh.setPlaceholdertype("CP");
				BusyhourFactory prevbhF = new BusyhourFactory(
						this.dwhrepRockFactory, prevbh);

				// fetch the installed (new) techpacks product busyhours
				final Busyhour installedbh = new Busyhour(
						this.dwhrepRockFactory);
				installedbh.setVersionid(techPackVersionID);
				installedbh.setPlaceholdertype("CP");
				BusyhourFactory installedbhF = new BusyhourFactory(
						this.dwhrepRockFactory, installedbh);
				
				
				//loop all new TP busyhours match them with old TP busyhours
				for (Busyhour oldBH : prevbhF.get()) {
					for (Busyhour newBH : installedbhF.get()) {
						if (oldBH.getBhlevel().equals(newBH.getBhlevel())
							&& oldBH.getBhtype().equals(newBH.getBhtype())
							&& !oldBH.getBhcriteria().equals("")) {
						
							//need to set all the columns
							//get a list of all the setters allowed on Busyhour
							copy(oldBH, newBH, bannedSetters);
							newBH.saveToDB();
							updatedCustomBusyhours++;
							break;
						}
					}
				}
			}
				
			else {
				logger.info("No previous tecpack installed, no need to copy Custom Busyhour(s).");
			}
			
			logger.info("Copied "+updatedCustomBusyhours+" Custom Busyhour(s).");
			
		} catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Copying Custom Busyhours failed.", e);
		}
		return updatedCustomBusyhours;
	}

	/**
	 * Copies the Custom BusyhourRankkeys from old TP to new TP
	 * @return
	 * @throws Exception 
	 */
	private int copyCustomBusyhourRankkeys() throws Exception {
		int copiedCustomBHRankkeys = 0;

		//Add the name of the banned setters in lowercase!
		ArrayList<String> bannedSetters = new ArrayList<String>();
		bannedSetters.add("setversionid");
		bannedSetters.add("settargetversionid");

		try {
			logger.info("Copying Custom BusyhourRankkeys...");
			
			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);

			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null) {

				// fetch the previous (old) techpacks custom Busyhourrankkeys
				String prevversionid = preTPActivation.getVersionid();
				final Busyhourrankkeys oldBHRankKeysSearch = new Busyhourrankkeys(this.dwhrepRockFactory);
				oldBHRankKeysSearch.setVersionid(prevversionid);
				BusyhourrankkeysFactory oldBHRankkeysFactory = new BusyhourrankkeysFactory(
						this.dwhrepRockFactory, oldBHRankKeysSearch);
								
				//loop all new TP busyhours match them with old TP busyhours
				for (Busyhourrankkeys oldBHRankkeys : oldBHRankkeysFactory.get()) {
					if(oldBHRankkeys.getBhtype().startsWith("CP")){ //only look at the CP's
						Busyhourrankkeys copiedBHRankkeys = new Busyhourrankkeys(this.dwhrepRockFactory);
						copy(oldBHRankkeys, copiedBHRankkeys, bannedSetters);
						copiedBHRankkeys.setVersionid(techPackVersionID);//TODO: Look at this for custom TP
						
						//need to get the targetVersionID. This can be obtained from the Busyhour table.
						final Busyhour busyhour = new Busyhour(this.dwhrepRockFactory);
						busyhour.setVersionid(techPackVersionID);
						busyhour.setBhlevel(copiedBHRankkeys.getBhlevel());
						busyhour.setBhtype(copiedBHRankkeys.getBhtype());
						final BusyhourFactory busyhourFactory = new BusyhourFactory(this.dwhrepRockFactory, busyhour);
						
						if(busyhourFactory.size() == 0){
							throw new Exception("There is no entry in the Busyhour for: versionID="+techPackVersionID+ ", BHLevel="+copiedBHRankkeys.getBhlevel()+ ", BHType="+copiedBHRankkeys.getBhtype());
						}
						
						String targetVersionID = busyhourFactory.getElementAt(0).getTargetversionid();

						//set the TargetversionID into the copied RankKeys entry.
						copiedBHRankkeys.setTargetversionid(targetVersionID);

						copiedBHRankkeys.saveToDB();
						copiedCustomBHRankkeys++;
					}
				}
			}
				
			else {
				logger.info("No previous tecpack installed, no need to copy Custom BusyhourRankkeys.");
			}
			
			logger.info("Copied "+copiedCustomBHRankkeys+" Custom BusyhourRankkeys.");
			
		} catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Copying Custom BusyhourRankkeys failed.", e);
		}
		return copiedCustomBHRankkeys;
	}

	
	/**
	 * Copies the Custom BusyhourSource from old TP to new TP
	 * @return
	 * @throws Exception 
	 */
	private int copyCustomBusyhourSource() throws Exception {
		int copiedCustomBHSource = 0;
		//Add the name of the banned setters in lowercase!
		ArrayList<String> bannedSetters = new ArrayList<String>();
		bannedSetters.add("setversionid");
		bannedSetters.add("settargetversionid");

		try {
			logger.info("Copying Custom BusyhourSource...");
			
			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);

			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null) {

				// fetch the previous (old) techpacks custom Busyhourrankkeys
				String prevversionid = preTPActivation.getVersionid();
				final Busyhoursource oldBHSourceSearch = new Busyhoursource(this.dwhrepRockFactory);
				oldBHSourceSearch.setVersionid(prevversionid);
				BusyhoursourceFactory oldBHSourceFactory = new BusyhoursourceFactory(
						this.dwhrepRockFactory, oldBHSourceSearch);
								
				//loop all new TP busyhours match them with old TP busyhours
				for (Busyhoursource oldBHSource : oldBHSourceFactory.get()) {
					if(oldBHSource.getBhtype().startsWith("CP")){ //only look at the CP's
						Busyhoursource copiedBHSource = new Busyhoursource(this.dwhrepRockFactory);
						copy(oldBHSource, copiedBHSource, bannedSetters);
						copiedBHSource.setVersionid(techPackVersionID);
						
						//need to get the targetVersionID. This can be obtained from the Busyhour table.
						final Busyhour busyhour = new Busyhour(this.dwhrepRockFactory);
						busyhour.setVersionid(techPackVersionID);
						busyhour.setBhlevel(copiedBHSource.getBhlevel());
						busyhour.setBhtype(copiedBHSource.getBhtype());
						final BusyhourFactory busyhourFactory = new BusyhourFactory(this.dwhrepRockFactory, busyhour);
						
						if(busyhourFactory.size() == 0){
							throw new Exception("There is no entry in the Busyhour for: versionID="+techPackVersionID+ ", BHLevel="+copiedBHSource.getBhlevel()+ ", BHType="+copiedBHSource.getBhtype());
						}
						
						String targetVersionID = busyhourFactory.getElementAt(0).getTargetversionid();						
						copiedBHSource.setTargetversionid(targetVersionID);
						copiedBHSource.saveToDB();
						copiedCustomBHSource++;
					}
				}
			}
				
			else {
				logger.info("No previous tecpack installed, no need to copy Custom BusyhourSource.");
			}
			
			logger.info("Copied "+copiedCustomBHSource+" Custom BusyhourSource(s).");
			
		} catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Copying Custom BusyhourSource failed.", e);
		}
		return copiedCustomBHSource;
	}

	

	/**
	 * This method copies the CustomBusyhourMappings from old TP to new TP.
	 * @return
	 * @throws Exception 
	 */
	private int copyCustomBusyhourmapping() throws Exception {
		int copiedCustomBusyhourmapping = 0;

		//Add the name of the banned setters in lowercase!
		ArrayList<String> bannedSetters = new ArrayList<String>();
		bannedSetters.add("setversionid");
		bannedSetters.add("settypeid");
		bannedSetters.add("settargetversionid");
		
		try {
			logger.info("Copying Custom Busyhourmapping...");
			
			Tpactivation preTPActivation = getPredecessorTPActivation(techPackName);

			// if targetTPActivation is null (no previously activated techpacks
			// in system) no need to do anything
			if (preTPActivation != null) {

				// fetch the previous (old) techpacks custom busyhourmapping
				String prevversionid = preTPActivation.getVersionid();
				final Busyhourmapping oldBHMappingSearch = new Busyhourmapping(this.dwhrepRockFactory);
				oldBHMappingSearch.setVersionid(prevversionid);
				BusyhourmappingFactory oldBHMFactory = new BusyhourmappingFactory(
						this.dwhrepRockFactory, oldBHMappingSearch);

				// fetch the installed (new) techpacks custom busyhourmapping
				final Busyhourmapping newBHMappingSearch = new Busyhourmapping(this.dwhrepRockFactory);
				newBHMappingSearch.setVersionid(techPackVersionID);
				BusyhourmappingFactory newBHMFactory = new BusyhourmappingFactory(
						this.dwhrepRockFactory, newBHMappingSearch);
				

				//loop all new TP busyhourMapping match them with old TP busyhourMapping
				for (Busyhourmapping oldBHMapping : oldBHMFactory.get()) {
					if(oldBHMapping.getBhtype().startsWith("CP")){ //only look at CP*
						for (Busyhourmapping newBHMapping : newBHMFactory.get()) {
							//need to massage the TypeId of the oldObject so that it can be used to match 
							//against the newObject.This is needed because someone thought it was a 
							//good idea to concatenate a key value with another value!
							oldBHMapping.setTypeid(oldBHMapping.getTypeid().replace(prevversionid, techPackVersionID));
							if (oldBHMapping.getBhlevel().equals(newBHMapping.getBhlevel())
									&& oldBHMapping.getBhtype().equals(newBHMapping.getBhtype())
									&& oldBHMapping.getBhobject().equals(newBHMapping.getBhobject())
									&& oldBHMapping.getTypeid().equals(newBHMapping.getTypeid())) {
								//need to set all the columns
								//get a list of all the setters allowed on Busyhour
								copy(oldBHMapping, newBHMapping, bannedSetters);
								newBHMapping.saveToDB();
								copiedCustomBusyhourmapping++;
								break;
							}
						}
					}
				}
			}
				
			else {
				logger.info("No previous tecpack installed, no need to copy Custom Busyhourmapping.");
			}
			
			logger.info("Copied "+copiedCustomBusyhourmapping+" Custom Busyhourmapping(s).");
			
		} catch (Exception e) {
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			e.printStackTrace();
			throw new Exception(
					"Copying Custom Busyhourmapping failed.", e);
		}
		return copiedCustomBusyhourmapping;
  }
	

	public String getTechPackName() {
		return techPackName;
	}

	public void setTechPackName(String techPackName) {
		this.techPackName = techPackName;
	}

	public String getTechPackMetadataVersion() {
		return String.valueOf(techPackMetadataVersion);
	}

	public void setTechPackMetadataVersion(String techPackMetadataVersion) {
		this.techPackMetadataVersion = Integer.parseInt(techPackMetadataVersion);
	}

	public String getBuildNumber() {
		return  String.valueOf(buildNumber);
	}

	public void setBuildNumber(String buildNumber) {
		this.buildNumber = Integer.parseInt(buildNumber);
	}

	public String getTechPackVersion() {
		return techPackVersion;
	}

	public void setTechPackVersion(String techPackVersion) {
		this.techPackVersion = techPackVersion;
	}

	/**
	 * Find all the setter methods from the given Class. 
	 * The returned Methods do not include setter methods from the "banned list". 
	 * The "banned list" are methods which are not columns in the table 
	 * @param className
	 * @return
	 */
	public ArrayList<Method> findSetterMethods(Class className) {
		ArrayList<String> bannedSetters = new ArrayList<String>();
		bannedSetters.add("setModifiedColumns");
		bannedSetters.add("setcolumnsAndSequences");
		bannedSetters.add("setDefaults");
		bannedSetters.add("setNewItem");
		bannedSetters.add("setValidateData");
		bannedSetters.add("setOriginal");
		
		ArrayList<Method> setterMethods = new ArrayList<Method>();
		
		for(Method m:className.getDeclaredMethods()){
			if(m.getName().startsWith("set")){
				if(!bannedSetters.contains(m.getName())){
					setterMethods.add(m);
				}
			}
		}
		return setterMethods;
	}


	/**
	 * This method takes two Table Objects (old and new) and copies one to the other.
	 * The old versionid is not copied to the new Table Object.
	 * @param oldTableObject
	 * @param newTableObject
	 * @param bannedSetters
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	private void copy(Object oldTableObject, Object newTableObject, ArrayList<String> bannedSetters)
			throws NoSuchMethodException, IllegalAccessException,
			InvocationTargetException {
		ArrayList<Method> setters = findSetterMethods(newTableObject.getClass());
		Iterator<Method> methodIterator = setters.iterator();

		while (methodIterator.hasNext()) {
			Method setter = methodIterator.next();
			if(bannedSetters.contains(setter.getName().toLowerCase())){
				//don't copy, this is a banned setter!
			}else{
				//copy
				String getterMethodName = setter.getName().replaceFirst("set",
						"get");
				Method getter = newTableObject.getClass().getDeclaredMethod(
						getterMethodName, null);

				Object o = getter.invoke(oldTableObject, (Object[]) null);
				
				if (o instanceof Integer) {
					setter.invoke(newTableObject, (Integer) o);
				} else if (o instanceof Long) {
					setter.invoke(newTableObject, (Long) o);
				} else if (o instanceof String) {
					setter.invoke(newTableObject, (String) o);
				}
			}
		}
	}

	/**
	 * This method is used to check if the objects are equal based on the Primary key.
	 * @param oldTable
	 * @param newTable
	 * @return
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public boolean isPrimaryKeyMatch(Object oldTable,
			Object newTable) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		//need to get the PrimaryKey List from one of the tables.
		Method getPrimaryKeys = oldTable.getClass().getDeclaredMethod("getprimaryKeyNames", null);
		String[] list = (String[])getPrimaryKeys.invoke(oldTable, null);

		Method[] allMethods = oldTable.getClass().getDeclaredMethods();
		//Iterate over the list of methods and get the primary Key methods.
		for(Method method: allMethods){
			String tmp = method.getName();
			if(tmp.startsWith("get")){
				tmp = tmp.replaceFirst("get", ""); //remove the "get" part.
				for(String s: list){
					if(s.equalsIgnoreCase(tmp)){
						//we have found the method to allow us to fetch one of the primary keys.
						if(method.invoke(oldTable, null) != method.invoke(newTable, null)){
							return false;
						}
					}
				}
			}
		}
		return true;
	}

}
