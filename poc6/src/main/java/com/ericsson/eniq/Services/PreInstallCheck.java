package com.ericsson.eniq.Services;



import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;


import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_schedulings;
import com.distocraft.dc5000.etl.rock.Meta_schedulingsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.repository.dwhrep.Datainterface;
import com.distocraft.dc5000.repository.dwhrep.DatainterfaceFactory;
import com.distocraft.dc5000.repository.dwhrep.Interfacedependency;
import com.distocraft.dc5000.repository.dwhrep.InterfacedependencyFactory;
import com.distocraft.dc5000.repository.dwhrep.Interfacemeasurement;
import com.distocraft.dc5000.repository.dwhrep.InterfacemeasurementFactory;
import com.distocraft.dc5000.repository.dwhrep.Interfacetechpacks;
import com.distocraft.dc5000.repository.dwhrep.InterfacetechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Mztechpacks;
import com.distocraft.dc5000.repository.dwhrep.MztechpacksFactory;
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;
import com.distocraft.dc5000.repository.dwhrep.Versioning;
import com.distocraft.dc5000.repository.dwhrep.VersioningFactory;


/**
 * This is a custom made ANT task that checks if the tech pack installer file is valid. It also does other checks before
 * the installation process can begin.
 * 
 * @author berggren
 */

@Component
public class PreInstallCheck {

	private transient String tpContentPath = "";

	private String tpDir = "";
	
	private String checkPrevMzTPInstall = "";

	private transient String techPackName = "";

	private transient int tpMetadataVers = 1;

	private transient String techPackVersion = "";

	private transient String DGtechpackType = "";

	private String cwd = "";

	private transient RockFactory etlrepRockFactory = null;

	private transient RockFactory dwhrepRockFactory = null;

	private final HashMap<String, String> requiredTechPackInstallations = new HashMap<String, String>();

	private String buildNumber = "";

	private String checkForRequiredTechPacks = "true";

	private String binDirectory = null;

	private String licenseName = "";

	

	private boolean forceInstall = false;



	private String currentTPProdNum = "";

	private static final String BOUNIVERSE_PATH = "/eniq/sw/installer/bouniverses/";

	private final Properties props = new Properties();

  private final String NIQ_INI_FILENAME = "niq.ini";

  private final String CONF_DIR = "/eniq/sw/conf/";
  


  private String tpName;

private String tpDirectory;

private static final Logger logger = LogManager.getLogger(PreInstallCheck.class);
  
	private static Long oldPollDelay = Long.valueOf(0);


	public static Long getoldPollDelay() {
		return oldPollDelay;
	}
	
	public PreInstallCheck()
	{
		
	}
	public PreInstallCheck(String tpName,String tpDirectory)
	{
		this.tpName=tpName;
		this.tpDirectory=tpDirectory;
	}

	/**
	 * This function starts the checking of the installation file.
	 */

	public void execute() throws Exception {
		try{
		
		
		logger.info("Checking connection to database...............");
		
		GetDatabaseDetails getdb=new GetDatabaseDetails();

		final Map<String, String> databaseConnectionDetails = getdb.getDatabaseConnectionDetails();

		// Create the connection to the etlrep.
		this.etlrepRockFactory = getdb.createEtlrepRockFactory(databaseConnectionDetails);
		// Create also the connection to dwhrep.
		this.dwhrepRockFactory=getdb.createDwhrepRockFactory(databaseConnectionDetails);

		logger.info("Connections to database created.");

		readTechPackVersionFile();

		logger.info("This tech pack uses metadata version " + this.tpMetadataVers);

		if (this.checkForRequiredTechPacks.equalsIgnoreCase("true")) {
			logger.info("Checking for required tech packs.");
			checkRequiredTechPackInstallations(this.requiredTechPackInstallations);
			logger.info("Checking for required tech packs finished.");
		} else {
			logger.info("Checking for required tech packs skipped.");
		}

		// Check if the tpi-file is a tech pack or interface or MZ techpack
		final boolean installingTechPack = checkForTechPackInstallation();
		final boolean installingInterface = checkForInterfaceInstallation();
		final boolean installingMZTechPack = checkForMZTechPackInstallation();

	

		logger.info("installingTechPack  value :" + installingTechPack);
		logger.info("installingMZTechPack  value :" + installingMZTechPack);
		if (installingTechPack || installingMZTechPack) {
			String techpackType = "";
			if (!installingMZTechPack) {
				techpackType = readMetadataValue("TP_TYPE");
			}
			checkForPrevTPInstallation();	
		} else if (installingInterface) {
			if (forceInstall) {
				logger.info("Skipping version check");
			} else {
				this.checkForPrevIntfInstallation();
			}
			
			removeIntfMetadata();

			// Remove the sets of this interface.
			removeIntfSets();
			logger.info("Previously installed interface metadata and sets removed successfully.");

	
		}

   
	
	}finally{
		if(this.dwhrepRockFactory != null){
	    	try {
				this.dwhrepRockFactory.getConnection().close();
			} catch (SQLException e) {
			}
    	}
    	if(this.etlrepRockFactory != null){
	    	try {
				this.etlrepRockFactory.getConnection().close();
			} catch (SQLException e) {
			}
    	}
	}
}



  /**
   * Appends a line of text to a file.
   * This will create the file if it doesn't exist.
   * @param filename      The file to write to.
   * @param append        If true, append to the file.
   * @param textToAdd     The text to add.
   * @throws IOException  Thrown if there is a file problem.
   */
  protected void appendLineToFile(final String filename, final boolean append, final String textToAdd)
      throws IOException {
    Writer output = new BufferedWriter(new FileWriter(filename, append));
    output.append(textToAdd);
    output.flush();
    output.close();
	}
  
  /**
   * Get the filename for the list of installed techpacks from niq.ini.
   * This is the file that stores the list of techpacks and interfaces
   * that are actually installed or upgraded, and not skipped over
   * because they are already installed.
   * 
   * @param   iniGet    Reference to an INIGet object.
   * @return  filename  The name of the installed techpacks file.
   *                    Return the default if we can't get the filename from niq.ini.
   */
 

  /**
   * Get the niq.ini file.
   * @return iniFile  Reference to the niq.ini file object.
   */
  protected File getNiqIniFile() {
    final File iniFile = new File(CONF_DIR, NIQ_INI_FILENAME);
    return iniFile;
  }

	
	/**
	 * This function reads up the TECH_PACK_NAME/install/version.properties file of the tech pack and parses the values
	 * to class variables and ANT project properties.
	 * 
	 * @throws Exception
	 *             On Errors
	 */
	private void readTechPackVersionFile() throws Exception {

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
			currentTPProdNum = props.getProperty("product.number", "");

			if ((this.techPackName == null) || (this.techPackName.length() <= 0)) {
				throw new Exception(
						"Required entry tech_pack.name was not found from version.properties file. Please check tech pack's version.properties file. Tech pack installation aborted.");
			}
			
			this.techPackVersion = props.getProperty("tech_pack.version");

			if ((this.techPackVersion == null) || (this.techPackVersion.length() <= 0)) {
				throw new Exception(
						"Required entry tech_pack.version was not found from version.properties. Please check tech pack's version.properties file. Tech pack installation aborted.");
			}
			
		
			this.DGtechpackType = props.getProperty("tech_pack.type");

			if ((this.DGtechpackType == null) || (this.DGtechpackType.length() <= 0)) {
				// logger.info("Not A DataGen TechPack");
				DGtechpackType = "MZ";
			}
		
		

			try {
				this.tpMetadataVers = Integer.parseInt(props.getProperty("tech_pack.metadata_version"));
			} catch (final Exception e) {
				logger.info("Techpack metadata version not propely set. Assuming version 1.");
			}

			
			this.buildNumber = props.getProperty("build.number");

			if ((this.buildNumber == null) || (this.buildNumber.length() <= 0)) {
				throw new Exception(
						"Required entry build.number was not found from version.properties. Please check tech pack's version.properties file. Tech pack installation aborted.");
			}
			
			this.licenseName = props.getProperty("license.name");
			

			for (final Object o : props.keySet()) {
				final String key = (String) o;

				if (key.startsWith("required_tech_packs")) {
					final String requiredTechPackName = key.substring(key.indexOf(".") + 1);
					String requiredTechPackVersion = props.getProperty(key);

					if (requiredTechPackVersion.indexOf("_b") > 0) {
						requiredTechPackVersion = requiredTechPackVersion.substring(0,
								requiredTechPackVersion.indexOf("_b"));
					}

					logger.info("Techpack " + requiredTechPackName + " version " + requiredTechPackVersion
							+ " required");
					this.requiredTechPackInstallations.put(requiredTechPackName, requiredTechPackVersion);
				}
			}
		}  catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Reading of file " + targetFilePath + " failed.", e);
		}
	}

	/**
	 * Returns the newest installed COA-number and R-state version of given techpack or null if no TP with given name
	 * exist in the DB.
	 * 
	 * @param tpName
	 *            Tech Pack Name
	 * @return ETLREP.Versioning Row
	 * @throws java.sql.SQLException
	 *             DB Errors
	 * @throws ssc.rockfactory.RockException
	 *             DB Connection Errors
	 */
	private Versioning getTargetVersioning(final String tpName) throws RockException, SQLException {
		Versioning latestVersioning = null;
		final Versioning whereVersioning = new Versioning(this.dwhrepRockFactory);
		whereVersioning.setTechpack_name(tpName);

		try {
			final VersioningFactory versioningFactory = new VersioningFactory(this.dwhrepRockFactory, whereVersioning);
			final Vector<Versioning> targetVersioningVector = versioningFactory.get();

			if (targetVersioningVector.size() > 0) {
				Collections.sort(targetVersioningVector, new VersionComparator());
				latestVersioning = targetVersioningVector.lastElement();
			}

		} catch (final SQLException e) {
			throw e;
		} catch (final RockException e) {
			throw e;
		} catch (final Exception e) {
			return latestVersioning;
		}
		return latestVersioning;
	}

	class VersionComparator implements Comparator<Versioning> {
		@Override
		public int compare(final Versioning vers1, final Versioning vers2) {
			// Cannot compare different techpacks
			if (!vers1.getTechpack_name().equalsIgnoreCase(vers2.getTechpack_name())) {
				return 0;
			}

			// first compare product numbers
			final String prod1 = vers1.getProduct_number() == null ? "" : vers1.getProduct_number();
			final String prod2 = vers2.getProduct_number() == null ? "" : vers2.getProduct_number();
			// compare only if they differ
			if (!prod1.equalsIgnoreCase(prod2)) {
					final int result = compareProductNumbers(prod1, prod2);
					if (result != 2){
						return result;
					}
			}

			// product numbers did not differ, so need to compare r-states
			final String tpVers1 = vers1.getTechpack_version();
			final String tpVers2 = vers2.getTechpack_version();
			final String r1 = tpVers1.contains("_") ? tpVers1.substring(0, tpVers1.indexOf("_")) : tpVers1;
			final String r2 = tpVers2.contains("_") ? tpVers2.substring(0, tpVers2.indexOf("_")) : tpVers2;
			// compare rStates only if they differ
			if (!r1.equalsIgnoreCase(r2)) {
				final int result = compareRstates(r1, r2);
				return (result == 1 ? 1 : -1);
			}

			// R-state didn't differ either, so compare build number
			final String b1 = tpVers1.contains("_") ? tpVers1.substring(tpVers1.indexOf("_") + 1) : "";
			final String b2 = tpVers2.contains("_") ? tpVers2.substring(tpVers2.indexOf("_") + 1) : "";
			if (!b1.equalsIgnoreCase(b2)) {
				final Integer bNum1 = b1.equalsIgnoreCase("") ? 0 : Integer.parseInt(b1.substring(b1.indexOf("b") + 1));
				final Integer bNum2 = b2.equalsIgnoreCase("") ? 0 : Integer.parseInt(b2.substring(b2.indexOf("b") + 1));
				return bNum1 - bNum2;
			}

			return 0;
		}

	}

	/**
	 * This function checks that the required techpacks are installed. If at least one required techpack is not
	 * installed, Exception is thrown. Check is performed agains versioning.techpack_version.
	 * 
	 * @param requiredTechPackInstallations
	 *            List of tech packs to check
	 * @throws Exception
	 *             On Errors
	 */
	private void checkRequiredTechPackInstallations(final HashMap<String, String> requiredTechPackInstallations)
			throws Exception {

		for (final String requiredTechPackName : requiredTechPackInstallations.keySet()) {
			String requiredTechPackVersion = requiredTechPackInstallations.get(requiredTechPackName);
			String requiredProdNum = "";

			if (requiredTechPackVersion.contains(":")) {
				// The COA-number is in the required techpacks version. Parse it
				// out
				// from there.
				final String[] splittedTPVersion = requiredTechPackVersion.split(":");
				requiredProdNum = splittedTPVersion[0];
				requiredTechPackVersion = splittedTPVersion[1];
			} else {
				// Assume that the required techpack has old coa-number.
			}

			try {
				final Versioning targetVersioning = getTargetVersioning(requiredTechPackName);

				if (targetVersioning == null) {
					logger.info("Required tech pack " + requiredTechPackName + " of at least version "
							+ requiredTechPackVersion
							+ " is not found. Please install required tech pack before installation can continue.");
					throw new Exception("Installation of tech pack failed to missing dependency package.");
				} else {
					String techPackVersion = targetVersioning.getTechpack_version();
					final String techPackProdNum = targetVersioning.getProduct_number();

					if (requiredProdNum.equalsIgnoreCase("") && techPackProdNum.contains("/")) {
						// It's ok to install this TP. Not really need to
						// compare RStates in
						// this situation.
						logger.info("Newer version (according to COA-number) " + requiredTechPackVersion
								+ " of required tech pack " + requiredTechPackName
								+ " is already installed. Installation can continue.");
					} else if (!requiredProdNum.equalsIgnoreCase("")) { // NOPMD
						// There is a product number in the version information.
						final Integer result = compareProductNumbers(requiredProdNum, techPackProdNum);

						if (result == 1) {
							logger.info("Required tech pack " + requiredTechPackName
									+ " has older version (according to COA-number) " + techPackProdNum
									+ " installed. Please update it to at least to version " + requiredProdNum
									+ " before installation can continue.");
							throw new Exception("Installation of tech pack failed.");
						} else if (result == -1) {
							logger.info("Newer version (according to COA-number) " + requiredTechPackVersion
									+ " of required tech pack " + requiredTechPackName
									+ " is already installed. Installation can continue.");
						} else {
							// Product numbers are the same. Start comparing the
							// RStates.
							// Drop off the _b123 if it exists in the techpack's
							// version.
							if (techPackVersion.indexOf("_") > 0) {
								techPackVersion = techPackVersion.substring(0, techPackVersion.lastIndexOf("_"));
							}

							final Integer rstateCompResult = compareRstates(techPackVersion, requiredTechPackVersion);

							if ((rstateCompResult == 0) || (rstateCompResult == 1)) {
								// The required Rstate is the same or newer than
								// required.
								logger.info("Newer or the same version " + requiredTechPackVersion
										+ " of required tech pack " + requiredTechPackName
										+ " is already installed. Installation can continue.");
							} else {
								logger.info("Required tech pack " + requiredTechPackName + " has older version "
										+ techPackVersion + " installed. Please update it to at least to version "
										+ requiredTechPackVersion + " before installation can continue.");
								throw new Exception("Installation of tech pack failed.");
							}

						}

					} else {
						// Product number is empty so compare the RStates
						// then...
						// Drop off the _b123 if it exists in the techpack's
						// version.
						if (techPackVersion.indexOf("_") > 0) {
							techPackVersion = techPackVersion.substring(0, techPackVersion.lastIndexOf("_"));
						}

						final Integer rstateCompResult = compareRstates(techPackVersion, requiredTechPackVersion);

						if ((rstateCompResult == 0) || (rstateCompResult == 1)) {
							// The required Rstate is the same or newer than
							// required.
							logger.info("Newer or the same version " + requiredTechPackVersion
									+ " of required tech pack " + requiredTechPackName
									+ " is already installed. Installation can continue.");
						} else {
							logger.info("Required tech pack " + requiredTechPackName + " has older version "
									+ techPackVersion + " installed. Please update it to at least to version "
									+ requiredTechPackVersion + " before installation can continue.");
							throw new Exception("Installation of tech pack failed.");
						}

					}

				}
			} catch (final RockException e) {
				e.printStackTrace();
				throw new Exception("Checking of required tech packs failed to RockException.", e);
			} catch (final SQLException e) {
				e.printStackTrace();
				throw new Exception("Checking of required tech packs failed to SQLException.", e);
			}
		}
	}

	/**
	 * This function parses the techpack content path and sets it in the class variable tpContentPath and ANT project
	 * property tpContentPath.
	 */
	

	public String getCurrentWorkingDirectory() {
		return cwd;
	}

	public void setCurrentWorkingDirectory(final String cwd) {
		this.cwd = cwd;
	}

	/**
	 * This function creates the RockFactory to dwhrep. The created RockFactory is inserted in class variable
	 * dwhrepRockFactory.
	 * @throws Exception 
	 */
	private void createDwhrepRockFactory() throws Exception {
		try {
			final Meta_databases whereMetaDatabases = new Meta_databases(this.etlrepRockFactory);
			whereMetaDatabases.setConnection_name("dwhrep");
			whereMetaDatabases.setType_name("USER");
			final Meta_databasesFactory metaDatabasesFactory = new Meta_databasesFactory(this.etlrepRockFactory,
					whereMetaDatabases);
			final Vector<Meta_databases> metaDatabases = metaDatabasesFactory.get();

			if ((metaDatabases != null) && (metaDatabases.size() == 1)) {
				final Meta_databases targetMetaDatabase = metaDatabases.get(0);
				this.dwhrepRockFactory = new RockFactory(targetMetaDatabase.getConnection_string(),
						targetMetaDatabase.getUsername(), targetMetaDatabase.getPassword(),
						etlrepRockFactory.getDriverName(), "PreinstallCheck", true);
				logger.info("DWHREP connection created");
			} else {
				throw new Exception(
						"Unable to connect metadata (No dwhrep or multiple dwhreps defined in Meta_databases)");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Creating database connection to dwhrep failed.", e);
		}
	}

	/**
	 * This function checks if a previous installation of this techpack exists. This check is performed against techpack
	 * build.number
	 * 
	 * @throws Exception
	 *             If previous or newer installation exists, ANT property "skipInstallationPhases" is set.
	 */

	public void checkForPrevTPInstallation() throws Exception {
		try {
			final Tpactivation whereTPActivation = new Tpactivation(this.dwhrepRockFactory);
			whereTPActivation.setTechpack_name(this.techPackName);
			final TpactivationFactory tpActivationFact = new TpactivationFactory(this.dwhrepRockFactory,
					whereTPActivation);
			final Vector<Tpactivation> tpActivationVect = tpActivationFact.get();

			if (tpActivationVect.size() > 0) {
				// Found activated version of this techpack
				final Tpactivation targetTPActivation = tpActivationVect.get(0);
				final String activatedVersionID = targetTPActivation.getVersionid();
				final Versioning whereVersioning = new Versioning(this.dwhrepRockFactory);
				whereVersioning.setTechpack_name(this.techPackName);
				whereVersioning.setVersionid(activatedVersionID);
				final VersioningFactory versioningFactory = new VersioningFactory(this.dwhrepRockFactory,
						whereVersioning);
				final Vector<Versioning> installedVersioningVector = versioningFactory.get();

				if (installedVersioningVector.size() > 0) {
					logger.info("Previous version of techpack found, version ID " + activatedVersionID);
					final Versioning installedTP = installedVersioningVector.get(0);
					final String installedTPVersion = installedTP.getTechpack_version();
					final String installedTPProdNum = installedTP.getProduct_number();

					final String currentTPProdNum = readMetadataValue("PROD_NUMBER");

					final Integer prodNumCompResult = compareProductNumbers(installedTPProdNum, currentTPProdNum);

					logger.info(this.techPackName + " " + installedTPProdNum + " is installed.");
					logger.info(this.techPackName + " " + currentTPProdNum
							+ " is being checked for a possible upgrade.");

					if (prodNumCompResult == 1) {
						System.out
								.println("Installed techpack has newer product number. This techpack will not be installed.");
						
					} else if (prodNumCompResult == -1) {
						System.out
								.println("Installed techpack has older product number. This techpack will be updated to "
										+ currentTPProdNum + ".");
					} else {
						logger.info("Product numbers of existing and upgrade techpack are equal.");
						String installedVersion;
						String installedBuild;
						final Pattern p3 = Pattern.compile(".+:\\(\\(\\d+\\)\\)");
						final Matcher m3 = p3.matcher(activatedVersionID);

						if (m3.matches()) {
							logger.info("Metadata version of previous techpack is 3");
							installedVersion = installedTPVersion;
							installedBuild = activatedVersionID.substring(activatedVersionID.indexOf(":") + 1);

							if (installedBuild.startsWith("((")) {
								installedBuild = installedBuild.substring(2);
							}

							if (installedBuild.endsWith("))")) {
								installedBuild = installedBuild.substring(0, installedBuild.length() - 2);
							}
						} else {
							final Pattern p4 = Pattern.compile(".+:\\(\\(.+\\)\\)");
							final Matcher m4 = p4.matcher(activatedVersionID);

							if (m4.matches()) {
								System.out
										.println("Cannot install techpack. Previous techpack version is a development techpack, please remove development techpack before intalling again.");
							
								return;
							} else {
								logger.info("Metadata version of previous techpack is < 3");
								installedVersion = installedTPVersion.substring(0, installedTPVersion.indexOf("_"));
								installedBuild = installedTPVersion.substring(
										(installedTPVersion.lastIndexOf("_b") + 2), installedTPVersion.length());
							}
						}
						final Integer rstateCompResult = compareRstates(this.techPackVersion, installedVersion);

						if (rstateCompResult == 0) {
							final Integer buildNumberInteger = Integer.valueOf(this.buildNumber);
							final Integer installedBuildNumberInteger = Integer.valueOf(installedBuild);

							if (buildNumberInteger > installedBuildNumberInteger) {
								logger.info("Older tech pack build b" + installedBuild
										+ " is installed. Tech pack will be updated to b" + this.buildNumber);
							} else {
								System.out
										.println("Newer or the same version b"
												+ installedBuild
												+ " of this tech pack already installed. This tech pack will not be installed. Skipping rest of the installation phases.");
								
							}
						} else if (rstateCompResult == 1) {
							logger.info("Older tech pack version " + installedVersion
									+ " is installed. Tech pack will be updated to version " + this.techPackVersion
									+ "_b" + this.buildNumber);
						} else if (rstateCompResult == 2) {
							// This tech pack is older than than the version in
							// database.
							logger.info("Newer version " + installedVersion
									+ " of this tech pack exist in database. This tech pack will not be installed.");
							
						} else {
							throw new Exception(
									"Could not compare the versions of techpacks. Installation has failed.");
						}

					}

				} else {
					logger.info("No previous version of this tech pack is installed.");
				}
			} else {
				logger.info("No previous version of this tech pack is installed and activated.");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Checking of required tech packs failed exceptionally.", e);
		}
	}

	/**
	 * This function checks if the tpi-file contains installation files for a techpack.
	 * 
	 * @return Returns true if the installation is for a techpack. Otherwise returns false.
	 */
	private boolean checkForMZTechPackInstallation() {

		File setDir;
		boolean techPackInstallation = false;
		if (this.techPackName.contains("M_E_LTEES")) {
			setDir = new File(this.tpDirectory+"/"+tpName + "/mz/wf");
		} else {
			setDir = new File(this.tpDirectory +"/"+tpName+ "/mz");
		}

		if (setDir.isDirectory() && setDir.canRead()) {
			final File[] files = setDir.listFiles();

			for (final File file : files) {
				if (file.isFile() && file.canRead()) {
					logger.info("It has been identified as tech pack.");
					techPackInstallation = true;
					break;
				}
			}
		}
		// Set the variable for the ANT script.
		return techPackInstallation;
	}

	/**
	 * This function checks if the tpi-file contains installation files for a techpack.
	 * 
	 * @return Returns true if the installation is for a techpack. Otherwise returns false.
	 */

	private boolean checkForTechPackInstallation() {

		boolean techPackInstallation = false;
		final File setDir = new File(this.tpDirectory+"/"+tpName + "/set");

		if (setDir.isDirectory() && setDir.canRead()) {
			final File[] files = setDir.listFiles();

			for (final File file : files) {
				if (file.isFile() && file.canRead()) {
					logger.info("It is has been identified as tech pack.");
					techPackInstallation = true;
					break;
				}
			}
		}
		// Set the variable for the ANT script.
		
		return techPackInstallation;
	}

	/**
	 * This function checks if the tpi-file contains installation files for an interface.
	 * 
	 * @return Returns true if the installation is for an interface. Otherwise returns false.
	 */
	private boolean checkForInterfaceInstallation() {
		boolean interfaceInstallation = false;
		final File interfaceDir = new File(this.tpDirectory+"/"+tpName + "/interface");

		if (interfaceDir.isDirectory() && interfaceDir.canRead()) {
			final File[] files = interfaceDir.listFiles();

			for (final File file : files) {

				if (file.isFile() && file.canRead()) {
					logger.info("It has been identified as an interface.");
					interfaceInstallation = true;
					break;
				}
			}
		}
		// Set the variable for the ANT script.
		
		return interfaceInstallation;
	}

	public String getCheckForRequiredTechPacks() {
		return checkForRequiredTechPacks;
	}

	public void setCheckForRequiredTechPacks(final String checkForRequiredTechPacks) {
		this.checkForRequiredTechPacks = checkForRequiredTechPacks;
	}

	@SuppressWarnings({ "UnusedDeclaration" })
	// Used in tasks xml
	public String getForceInstall() {
		return String.valueOf(forceInstall);
	}

	@SuppressWarnings({ "UnusedDeclaration" })
	// Used in tasks xml
	public void setForceInstall(final String force) {
		forceInstall = "true".equals(force);
	}

	/**
	 * This function checks if a previous installation of this interface exists.
	 * 
	 * @throws Exception
	 *             If previous or newer installation exists, ANT property "skipInstallationPhases" is set.
	 */
	public void checkForPrevIntfInstallation() throws Exception {
		try {

			final Meta_collection_sets whereMetaCollectionSets = new Meta_collection_sets(this.etlrepRockFactory);
			whereMetaCollectionSets.setCollection_set_name(this.techPackName);
			final Meta_collection_setsFactory metaCollSetsFactory = new Meta_collection_setsFactory(
					this.etlrepRockFactory, whereMetaCollectionSets, " ORDER BY VERSION_NUMBER DESC;");
			final Vector<Meta_collection_sets> metaCollSetsVect = metaCollSetsFactory.get();

			if (metaCollSetsVect.size() > 0) {
				final Meta_collection_sets targetMetaCollSet = metaCollSetsVect.get(0);
				final String targetIntfVersion = targetMetaCollSet.getVersion_number(); // R2A_b100 ( should be
																						// something like this)
				String targetVersion;

				if (targetIntfVersion.contains("_")) {
					targetVersion = targetIntfVersion.substring(0, targetIntfVersion.indexOf("_"));
				} else {
					// This needs to be changed, INTF_DC_E_x made with TP-IDE will have the version number as ((nn))
					// in the META_COLLECTION_SET table so we need to go to DataInterface to get the current RSTATE
					// thats installed.
					final Datainterface whereDataInterface = new Datainterface(this.dwhrepRockFactory);
					whereDataInterface.setInterfacename(this.techPackName);
					final DatainterfaceFactory dataInterfaceFact = new DatainterfaceFactory(this.dwhrepRockFactory,
							whereDataInterface);
					final Vector<Datainterface> datainterfaceSet = dataInterfaceFact.get();
					targetVersion = datainterfaceSet.get(0).getRstate();
					logger.info("RState not found in MCS, read from DI instead --> " + targetVersion);
				}

				String targetBuild;

				if (targetIntfVersion.lastIndexOf("_b") != -1) { // NOPMD
					targetBuild = targetIntfVersion.substring((targetIntfVersion.lastIndexOf("_b") + 2),
							targetIntfVersion.length());
				} else {
					final String regex = "\\(\\((\\d+)\\)\\)";
					final Pattern p = Pattern.compile(regex);
					final Matcher m = p.matcher(targetIntfVersion);
					if (m.matches()) {
						targetBuild = m.group(1);
					} else {
						targetBuild = "0";
						logger.info("Interface " + targetMetaCollSet.getCollection_set_name()
								+ " has wrong build number " + targetIntfVersion + ". Using value 0 as buildnumber.");
					}

				}
				final Integer rstateCompResult = compareRstates(this.techPackVersion, targetVersion);

				if (rstateCompResult == 0) {
					// The interface is of a same version. Check for a newer
					// build number.
					final Integer buildNumberInteger = Integer.valueOf(this.buildNumber);
					final Integer targetBuildNumberInteger = Integer.valueOf(targetBuild);

					if (buildNumberInteger > targetBuildNumberInteger) {
						// This interface is newer than the version in database.
						logger.info("Older interface build b" + targetBuild
								+ " is installed. Interface will be updated to b" + this.buildNumber);
					} else {
						// This interface is older than the version in database.
						System.out
								.println("Newer or the same version b"
										+ targetBuild
										+ " of this interface is already installed. This interface will not be installed. Skipping rest of the installation phases.");
						
					}
				} else if (rstateCompResult == 1) {
					// This interface is newer than the version in database.
					// Install this
					// interface.
					logger.info("Older interface version " + targetVersion
							+ " is installed. Interface will be updated to version " + this.techPackVersion + "_b"
							+ this.buildNumber);
				} else {
					// This interface is older than than the version in
					// database.
					logger.info("Newer version " + targetVersion
							+ " of this interface exists in database. This interface will not be installed.");
					
				}
			} else {
				// No previous version of interface is installed.
				logger.info("No previous version of this interface is installed.");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Checking for previous installed interfaces failed.", e);
		}
	}

	/**
	 * This function removes interface data from tables DataInterface, InterfaceMeasurement and InterfaceTechPacks.
	 * @throws java.lang.Exception 
	 */
	private void removeIntfMetadata() throws java.lang.Exception {

		try {

			logger.info("Starting metadata removal of interface " + this.techPackName + ".");
			final Interfacemeasurement whereIntfMeasurement = new Interfacemeasurement(this.dwhrepRockFactory);
			whereIntfMeasurement.setInterfacename(this.techPackName);
			final InterfacemeasurementFactory intfMeasurementFactory = new InterfacemeasurementFactory(
					this.dwhrepRockFactory, whereIntfMeasurement);
			final Vector<Interfacemeasurement> intfMeasVect = intfMeasurementFactory.get();

			for (final Interfacemeasurement currentIntfMeas : intfMeasVect) {
				currentIntfMeas.deleteDB();
			}

			final Interfacetechpacks whereIntfTechPacks = new Interfacetechpacks(this.dwhrepRockFactory);
			whereIntfTechPacks.setInterfacename(this.techPackName);
			final InterfacetechpacksFactory intfTechPacksFact = new InterfacetechpacksFactory(this.dwhrepRockFactory,
					whereIntfTechPacks);
			final Vector<Interfacetechpacks> intfTechPacks = intfTechPacksFact.get();

			for (final Interfacetechpacks currentIntfTechPacks : intfTechPacks) {
				currentIntfTechPacks.deleteDB();
			}

			final Interfacedependency whereIntfDep = new Interfacedependency(this.dwhrepRockFactory);
			whereIntfDep.setInterfacename(this.techPackName);
			final InterfacedependencyFactory intfDepFactory = new InterfacedependencyFactory(this.dwhrepRockFactory,
					whereIntfDep);
			final Vector<Interfacedependency> intfDep = intfDepFactory.get();

			for (final Interfacedependency currentIntfDep : intfDep) {
				currentIntfDep.deleteDB();
			}

			final Datainterface whereDataIntf = new Datainterface(this.dwhrepRockFactory);
			whereDataIntf.setInterfacename(this.techPackName);
			final DatainterfaceFactory dataIntfFactory = new DatainterfaceFactory(this.dwhrepRockFactory, whereDataIntf);
			final Vector<Datainterface> dataInterfaces = dataIntfFactory.get();

			for (final Datainterface currentDataIntf : dataInterfaces) {
				currentDataIntf.deleteDB();
			}

			logger.info("Metadata of interface " + this.techPackName + " removed succesfully.");
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Removing metadata of interface " + this.techPackName + " failed.", e);
		}
	}

	/**
	 * This function removes the sets of this interface and all activated OSS's interface sets.
	 * @throws Exception 
	 */
	private void removeIntfSets() throws Exception {

		try {
			final Meta_collection_sets whereCollSets = new Meta_collection_sets(this.etlrepRockFactory);
			final Meta_collection_setsFactory collSetsFactory = new Meta_collection_setsFactory(this.etlrepRockFactory,
					whereCollSets);
			final Vector<Meta_collection_sets> collSets = collSetsFactory.get();

			for (final Meta_collection_sets currentCollSet : collSets) {
				// Remove the interface sets. The interface's own sets and every
				// activated interface's sets.
				// Interface's own sets: INTF_DC_E_XYZ
				// Activated interface's sets are in format
				// INTF_DC_E_XYZ-OSS_NAME.

				if (currentCollSet.getCollection_set_name().equalsIgnoreCase(this.techPackName)
						|| currentCollSet.getCollection_set_name().startsWith(this.techPackName + "-")) {
					System.out
							.println("Deleting interface set " + currentCollSet.getCollection_set_name()
									+ " with CollectionSetID: " + currentCollSet.getCollection_set_id()
									+ " and it's contents.");

					if (currentCollSet.getEnabled_flag() != "N") {
						final Meta_schedulings whereMetaSche = new Meta_schedulings(this.etlrepRockFactory);
						whereMetaSche.setCollection_set_id(currentCollSet.getCollection_set_id());
						final Meta_schedulingsFactory metaSchedulingsFactory = new Meta_schedulingsFactory(
								this.etlrepRockFactory, whereMetaSche);
						final Vector<Meta_schedulings> metaSchedulings = metaSchedulingsFactory.get();
						for (final Meta_schedulings currentMetaSch : metaSchedulings) {
							if (currentMetaSch.getName().startsWith("TriggerAdapter_")) {
								if (currentMetaSch.getScheduling_min().longValue() > 0) {
									oldPollDelay = currentMetaSch.getScheduling_min();
								}
							}
						}// for
					}

					// This set is the currently installed interface's or some
					// activated
					// OSS's sets.
					// Delete the whole set including everything related to it.
					final Meta_collections whereColls = new Meta_collections(this.etlrepRockFactory);
					whereColls.setCollection_set_id(currentCollSet.getCollection_set_id());
					final Meta_collectionsFactory collFactory = new Meta_collectionsFactory(this.etlrepRockFactory,
							whereColls);
					final Vector<Meta_collections> collections = collFactory.get();

					for (final Meta_collections currentCollection : collections) {
						final Meta_transfer_actions whereTrActions = new Meta_transfer_actions(this.etlrepRockFactory);
						whereTrActions.setCollection_id(currentCollection.getCollection_id());
						whereTrActions.setCollection_set_id(currentCollSet.getCollection_set_id());
						final Meta_transfer_actionsFactory trActionsFactory = new Meta_transfer_actionsFactory(
								this.etlrepRockFactory, whereTrActions);
						final Vector<Meta_transfer_actions> trActions = trActionsFactory.get();

						for (final Meta_transfer_actions currTrAction : trActions) {
							currTrAction.deleteDB();
						}
						// Do not delete META_TRANSFER_BATCHES entries. This
						// will be done by
						// housekeeping set.

						/*
						 * Deleting the meta_schedulings entry - Fix for TR - HL364. The meta_schedulings refer back
						 * to meta_collection_sets.
						 */
						final Meta_schedulings whereMetaSchedulings = new Meta_schedulings(this.etlrepRockFactory);

						whereMetaSchedulings.setCollection_id(currentCollection.getCollection_id());

						whereMetaSchedulings.setCollection_set_id(currentCollSet.getCollection_set_id());
						final Meta_schedulingsFactory metaSchedulingsFactory = new Meta_schedulingsFactory(
								this.etlrepRockFactory, whereMetaSchedulings);
						final Vector<Meta_schedulings> metaSchedulings = metaSchedulingsFactory.get();

						for (final Meta_schedulings currentMetaScheduling : metaSchedulings) {

							currentMetaScheduling.deleteDB();
						}

						currentCollection.deleteDB();
					}

					currentCollSet.deleteDB();
				}
			}
			logger.info("Final Value for oldPollDelay: " + oldPollDelay);
		} catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Failed removing previous installations interface sets.", e);
		}
	}


	public String getBinDirectory() {
		return binDirectory;
	}

	public void setBinDirectory(final String binDirectory) {
		this.binDirectory = binDirectory;
	}

	/**
	 * This function creates a connection to the licensing cache and asks for a specific license.
	 * 
	 * @return Returns the return code. 0 = license is ok, > 0 means that license is not ok.
	 */

	


	/**
	 * This function compares two RStates. The RState format is "R19C". Returns 0 if the RStates are equal, returns 1 if
	 * the firstRstate is bigger, returns 2 if the secondRstate is bigger and returns -1 if the Rstates are in incorrect
	 * format.
	 * 
	 * @param firstRstate
	 *            First RSTATE
	 * @param secondRstate
	 *            Second RSTATE
	 * @return Integer 0 --> RSTATE are equal 1 --> First RSTATE > Second RSTATE 2 --> Second RSTATE > First RSTATE
	 */
	public Integer compareRstates(final String firstRstate, final String secondRstate) {
		// Use regexp to get the number value of RState.
		final Pattern pattern = Pattern.compile("\\d+");
		final Matcher matcher = pattern.matcher(firstRstate);

		if (!matcher.find()) {
			logger.info("Rstate " + firstRstate + " has invalid format.");
			return -1;
		}

		final String firstRstateNum = matcher.group(0);
		final Matcher matcher2 = pattern.matcher(secondRstate);

		final Pattern pattern2 = Pattern.compile(".$");
		final Matcher matcher3 = pattern2.matcher(firstRstate);

		if (!matcher3.find()) {
			logger.info("Rstate " + firstRstate + " has invalid format.");
			return -1;
		}

		if (!matcher2.find()) {
			logger.info("Rstate " + secondRstate + " has invalid format.");
			return -1;
		}
		final String firstRstateLastChar = matcher3.group(0);
		final Matcher matcher4 = pattern2.matcher(secondRstate);

		if (!matcher4.find()) {
			logger.info("Rstate " + secondRstate + " has invalid format.");
			return -1;
		}
		final String secondRstateLastChar = matcher4.group(0);
		final String secondRstateNum = matcher2.group(0);

		if (Integer.parseInt(firstRstateNum) == Integer.parseInt(secondRstateNum)) {
			// The RState numbers are equal.
			// Check the string after RState number which is bigger.
			if (firstRstateLastChar.compareTo(secondRstateLastChar) == 0) {
				return 0;
			} else if (firstRstateLastChar.compareTo(secondRstateLastChar) > 0) {
				return 1;
			} else {
				return 2;
			}
		} else {
			// Let the Rstate number decide which is bigger.
			if (Integer.parseInt(firstRstateNum) > Integer.parseInt(secondRstateNum)) {
				return 1;
			} else {
				return 2;
			}
		}
	}

	/**
	 * This function reads configuration of the RMI.
	 * 
	 * @throws Exception
	 *             On errors
	 */

	

	

	@SuppressWarnings({ "UnusedDeclaration" })
	public String getTpDir() {
		return tpDir;
	}

	@SuppressWarnings({ "UnusedDeclaration" })
	// Used in tasks xml
	public void setTpDir(final String tpDir) {
		this.tpDir = tpDir;
	}
	
	@SuppressWarnings({ "UnusedDeclaration" })
	public String getCheckPrevMzTPInstall() {
		return checkPrevMzTPInstall;
	}

	@SuppressWarnings({ "UnusedDeclaration" })
	// Used in tasks xml
	public void setCheckPrevMzTPInstall(final String checkPrevMzTPInstall) {
		this.checkPrevMzTPInstall = checkPrevMzTPInstall;
	}
	
	

	// Deletes all files and subdirectories under dir.
	// Returns true if all deletions were successful.
	// If a deletion fails, the method stops attempting to delete and returns
	// false.
	private boolean deleteDir(final File dir) {
		if (dir.isDirectory()) {
			final String[] files = dir.list();
			for (final String file : files) {
				final File tempFile = new File(dir, file);
				final boolean success = deleteDir(tempFile);
				if (!success) {
					logger.info("Unable to delete the file/directory: " + tempFile.getAbsolutePath());
				}
			}
		}
		// The directory is now empty so delete it
		return dir.delete();
	}

	/**
	 * This function reads the metadata file of the current techpack and determines what type of a techpack it is.
	 * 
	 * @param tgtMetadata
	 *            metadata to be read from the sql file. Can be either "TP_TYPE" or "PROD_NUMBER".
	 * @return Returns the type of the techpack.
	 */
	private String readMetadataValue(final String tgtMetadata) {

		FileInputStream fis = null;
		BufferedInputStream bis = null;
		BufferedReader bufr = null;

		// Hardcoding the positioning of TECHPACK_TYPE and PRODUCT_NUMBER based on
		// split function.
		// Easiest way to get techpack_type and product_number.
		// Needs change if a new column is added before techpack_type or product_number in Versioning table.

		final int TECHPACK_TYPE_POSITION = 9;
		final int PRODUCT_TYPE_POSITION = 11;

		try {
			final File metadataDir = new File(this.tpDirectory+"/"+tpName + "/sql");

			if (metadataDir.isDirectory() && metadataDir.canRead()) {
				final File[] sqlFiles = metadataDir.listFiles();

				for (final File currFile : sqlFiles) {
					if (currFile.isDirectory()) {
						continue;
					}
					fis = new FileInputStream(currFile);
					bis = new BufferedInputStream(fis);
					bufr = new BufferedReader(new InputStreamReader(bis));
					String line;

					while ((line = bufr.readLine()) != null) {
						if (tgtMetadata.equalsIgnoreCase("TP_TYPE")) {
							if (line.contains("techpack_type") || line.contains("TECHPACK_TYPE")) {
								// We have found the line where type of the
								// techpack is
								// specified.
								final String[] values = line.split("'");
								// Type of techpack is the second last entry in
								// the sql clause
								// on
								// the line.
								final String techpackType = values[TECHPACK_TYPE_POSITION];
								logger.info("Read techpacktype " + techpackType + " from techpack "
										+ this.techPackName);
								return techpackType;
							}
						} else if (tgtMetadata.equalsIgnoreCase("PROD_NUMBER")) {
							if (line.contains("product_number") || line.contains("PRODUCT_NUMBER")) {
								// We have found the line where product number
								// is specified.
								final String[] values = line.split("'");
								// Product number is the last entry in the sql
								// clause on
								// the line.
								final String prodNumber = values[PRODUCT_TYPE_POSITION];
								logger.info("Read product number " + prodNumber + " from techpack "
										+ this.techPackName);
								return prodNumber;
							}
						}
					}
				}
			} else {
				logger.info("This techpack needs a valid license in order to be install.");
				return "UNKNOWN";
			}

		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (final Exception e) {
					logger.info("Failed to close " + fis.toString());
				}
			}

			if (bis != null) {
				try {
					bis.close();
				} catch (final Exception e) {
					logger.info("Failed to close " + bis.toString());
				}
			}

			if (bufr != null) {
				try {
					bufr.close();
				} catch (final Exception e) {
					logger.info("Failed to close " + bufr.toString());
				}
			}

		}

		return "UNKNOWN";
	}

	/**
	 * This function compares two different product numbers and returns an Integer telling the comparison result.
	 * Returns 0 if the product numbers are equal, returns 1 if the first product number is bigger, returns -1 if the
	 * second product number is bigger and returns 2 if the product numbers are in incorrect format. Example product
	 * numbers could be: COA 123 456, COA 123 558/1, COA 121 981/3 etc.
	 * 
	 * @param oldProdNum
	 *            first product number (old one).
	 * @param newProdNum
	 *            second product number (new one).
	 * @return Returns the comparison result.
	 */
	private Integer compareProductNumbers(final String oldProdNum, final String newProdNum) {
		// If the old one does not contain "/" character and the new product
		// number does, upgrade is done every time.
		if (!oldProdNum.contains("/") && newProdNum.contains("/")) {
			return -1;
		}

		// If the old one contains "/" character and the new product number
		// doesn't,
		// then upgrade is never done.
		if (oldProdNum.contains("/") && !newProdNum.contains("/")) {
			return 1;
		}

		// If both old and new product numbers include "/" character, then
		// compare the number after the "/" character. Bigger number is newer
		// and should be updated.
		if (oldProdNum.contains("/") && newProdNum.contains("/")) {
			final Integer oldProdNumExtension = Integer.valueOf(oldProdNum.substring((oldProdNum.lastIndexOf("/") + 1),
					oldProdNum.length())); // Example
											// :
											// 1
			final Integer newProdNumExtension = Integer.valueOf(newProdNum.substring((newProdNum.lastIndexOf("/") + 1),
					newProdNum.length())); // Example
											// :
											// 2

			if (oldProdNumExtension > newProdNumExtension) {
				return 1;
			} else if (newProdNumExtension > oldProdNumExtension) {
				return -1;
			} else {
				return 0;
			}
		}

		if (oldProdNum.equalsIgnoreCase(newProdNum)) {
			return 0;
		}

		return 2;
	}

	public RockFactory getDwhrepRockFactory() {
		return dwhrepRockFactory;
	}

	public void setDwhrepRockFactory(final RockFactory dwhrepRockFactory) {
		this.dwhrepRockFactory = dwhrepRockFactory;
	}

	public RockFactory getEtlrepRockFactory() {
		return etlrepRockFactory;
	}

	public void setEtlrepRockFactory(final RockFactory etlrepRockFactory) {
		this.etlrepRockFactory = etlrepRockFactory;
	}

	/**
	 * This function checks if a previous installation of this techpack exists. This check is performed against techpack
	 * build.number
	 * 
	 * @throws Exception
	 *             If previous or newer installation exists, ANT property "skipInstallationPhases" is set.
	 */

	public int checkForPrevMzTPInstallation() throws Exception {
		try {
			final Mztechpacks whereTPActivation = new Mztechpacks(this.dwhrepRockFactory);
			whereTPActivation.setTechpack_name(this.techPackName);

			final MztechpacksFactory tpActivationFact = new MztechpacksFactory(this.dwhrepRockFactory,
					whereTPActivation);

			if (tpActivationFact.size() == 0) {
				logger.info("No Mz TP is installed earlier,Installation of" + this.techPackName
						+ " will continue.");
				return 0;
			}
			String activatedVersionID;
			String installedTPVersion;
			String installedTPProdNum;
			String installedTPName;
			String installedTPStatus;
			Integer prodNumCompResult;

			for (final Mztechpacks mzTpForPrint : tpActivationFact.get()) {
				installedTPName = mzTpForPrint.getTechpack_name();
				installedTPStatus = mzTpForPrint.getStatus();

				if (installedTPName.equalsIgnoreCase(this.techPackName) && installedTPStatus.equalsIgnoreCase("ACTIVE")) {
					activatedVersionID = mzTpForPrint.getVersionid();
					installedTPVersion = mzTpForPrint.getTechpack_version();
					installedTPProdNum = mzTpForPrint.getProduct_number();

					prodNumCompResult = compareProductNumbers(installedTPProdNum, currentTPProdNum);

					if (prodNumCompResult == 1) {
						System.out
								.println("Installed techpack has newer product number. This techpack will not be installed.");
						return 77;
					} else if (prodNumCompResult == -1) {
						System.out
								.println("Installed techpack has older product number. This techpack will be updated to "
										+ currentTPProdNum + ".");
						return 0;
					} else {
						logger.info("Product numbers of existing and upgrade techpack are equal.");
						String installedVersion;
						String installedBuild;
						final Pattern p3 = Pattern.compile(".+:\\(\\(\\d+\\)\\)");
						final Matcher m3 = p3.matcher(activatedVersionID);

						if (m3.matches()) {
							logger.info("Metadata version of previous techpack is 3");
							installedVersion = installedTPVersion;
							installedBuild = activatedVersionID.substring(activatedVersionID.indexOf(":") + 1);

							if (installedBuild.startsWith("((")) {
								installedBuild = installedBuild.substring(2);
							}

							if (installedBuild.endsWith("))")) {
								installedBuild = installedBuild.substring(0, installedBuild.length() - 2);
							}
						} else {
							final Pattern p4 = Pattern.compile(".+:\\(\\(.+\\)\\)");
							final Matcher m4 = p4.matcher(activatedVersionID);

							if (m4.matches()) {
								System.out
										.println("Cannot install techpack. Previous techpack version is a development techpack, please remove development techpack before intalling again.");
								return 78;
							} else {
								logger.info("Metadata version of previous techpack is < 3");
								installedVersion = installedTPVersion;
								installedBuild = installedTPVersion.substring(
										(installedTPVersion.lastIndexOf("_b") + 2), installedTPVersion.length());
							}
						}
						final Integer rstateCompResult = compareRstates(this.techPackVersion, installedVersion);
						if (rstateCompResult == 0) {
							final Integer buildNumberInteger = Integer.valueOf(this.buildNumber);
							final Integer installedBuildNumberInteger = Integer.valueOf(installedBuild);

							if (buildNumberInteger > installedBuildNumberInteger) {
								logger.info("Older tech pack build b" + installedBuild
										+ " is installed. Tech pack will be updated to b" + this.buildNumber);
								return 0;
							} else {
								System.out
										.println("Newer or the same version b"
												+ installedBuild
												+ " of this tech pack already installed. This tech pack will not be installed. Skipping rest of the installation phases.");
								return 79;
							}
						} else if (rstateCompResult == 1) {
							logger.info("Older tech pack version " + installedVersion
									+ " is installed. Tech pack will be updated to version " + this.techPackVersion
									+ "_b" + this.buildNumber);
							return 0;
						} else if (rstateCompResult == 2) {
							// This tech pack is older than than the version in
							// database.
							logger.info("Newer version " + installedVersion
									+ " of this tech pack exist in database. This tech pack will not be installed.");
							return 80;
						} else {

							throw new Exception(
									"Could not compare the versions of techpacks. Installation has failed.");
						}
					}
				}
			}
		} 
		catch (final Exception e) {
			e.printStackTrace();
			throw new Exception("Checking of required tech packs failed exceptionally.", e);
		}
		return 0;
	}


  
}