package com.ericsson.eniq.Services;

import java.io.*;

import java.sql.SQLException;
import java.util.*;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.distocraft.dc5000.repository.dwhrep.*;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
import org.apache.logging.log4j.LogManager;

@Component
public class TPInstallOrderer {

	private static final Logger logger = LogManager.getLogger(TPInstallOrderer.class);

	private static String techpackDirectory = null;

	private File tpDir = null;

	private transient RockFactory etlrepRockFactory = null;

	private transient RockFactory dwhrepRockFactory = null;

	private File targetFile = null;

	private FileInputStream versionFile = null;

	private String tpName;

	@Autowired
	private GetDatabaseDetails getDatabaseDetails;

	public TPInstallOrderer() {

	}

	public TPInstallOrderer(String tpNameInput, String techpackDirectoryInput) {
		tpName = tpNameInput;
		techpackDirectory = techpackDirectoryInput;
	}

	public int execute(String tpName, String techpackDirectory) {
		boolean resflag = false;
		try {

			if (techpackDirectory == null) {
				throw new Exception("parameter techpackDirectory has to be defined");
			}

			tpDir = new File(techpackDirectory);

			if (!tpDir.exists() || !tpDir.canRead()) {
				throw new Exception("Unable to read techpackDirectory");
			}

			logger.info("Checking connection to database...............");

			// GetDatabaseDetails getdb = new GetDatabaseDetails();
			final Map<String, String> databaseConnectionDetails = getDatabaseDetails.getDatabaseConnectionDetails();
			etlrepRockFactory = getDatabaseDetails.createEtlrepRockFactory(databaseConnectionDetails);
			this.dwhrepRockFactory = getDatabaseDetails.createDwhrepRockFactory(databaseConnectionDetails);

			logger.info("Connections to database created.");

			try {
				targetFile = new File(techpackDirectory + "/" + tpName + "/" + "install/version.properties");
				versionFile = new FileInputStream(targetFile);
			} catch (Exception e) {
				logger.info("Exception " + e);
			}

			final TPEntry tpe = loadTPE(tpName, versionFile);
			resflag = checkAlreadyInstalled(tpName, tpe);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				throw new Exception("Unexpected failure");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			try {
				if (etlrepRockFactory != null) {
					etlrepRockFactory.getConnection().close();
				}
				if (dwhrepRockFactory != null) {
					dwhrepRockFactory.getConnection().close();
				}
			} catch (final SQLException sqle) {
				System.out.print("Connection cleanup error - " + sqle.toString());
			}
			dwhrepRockFactory = null;
			etlrepRockFactory = null;
		}
		if (resflag) {
			return 0;
		} else {
			return 1;
		}
	}

	/**
	 * @param tp0
	 * @return
	 * @return
	 * @throws Exception
	 * @throws RockException
	 * @throws SQLException
	 */
	private boolean checkAlreadyInstalled(String techPackName, TPEntry tp0) throws Exception {
		boolean installflag = false;
		// TODO Auto-generated method stub
		try {

			final Tpactivation whereTPActivation = new Tpactivation(dwhrepRockFactory);
			whereTPActivation.setTechpack_name(techPackName);
			TpactivationFactory tpActivationFact;
			// tpActivationFact = new TpactivationFactory(dwhrepRockFactory,
			// whereTPActivation);
			// final Vector<Tpactivation> tpActivationVect = tpActivationFact.get();

			final Datainterface whereDataInterface = new Datainterface(dwhrepRockFactory);
			whereDataInterface.setInterfacename(techPackName);
//			final DatainterfaceFactory dataInterfaceFact = new DatainterfaceFactory(dwhrepRockFactory,
//					whereDataInterface);
//			final Vector<Datainterface> datainterfaceSet = dataInterfaceFact.get();

			//
//			if (!techPackName.startsWith("INTF")) {
//				installflag=checkTechPackInstalled(techPackName, tp0, tpActivationVect);
//			} else {
//				installflag=checkIntfAlreadyInstalled(techPackName, tp0, datainterfaceSet);
//			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Error reading installListFile", e);
		}
		return installflag;
	}

	/**
	 * @param techPackName
	 * @param tp0
	 * @param tpActivationVect
	 * @throws Exception
	 */
	private boolean checkTechPackInstalled(String techPackName, TPEntry tp0,
			final Vector<Tpactivation> tpActivationVect) throws Exception {
		boolean flag = false;
		if (tpActivationVect.size() > 0) {
			// Found activated version of this techpack
			final Tpactivation targetTPActivation = tpActivationVect.get(0);
			final String activatedVersionID = targetTPActivation.getVersionid();
			final Versioning whereVersioning = new Versioning(dwhrepRockFactory);
			whereVersioning.setTechpack_name(techPackName);
			whereVersioning.setVersionid(activatedVersionID);
			VersioningFactory versioningFactory;
			versioningFactory = new VersioningFactory(dwhrepRockFactory, whereVersioning);
			final Vector<Versioning> installedVersioningVector = versioningFactory.get();
			final Versioning installedTP = installedVersioningVector.get(0);
			String installedTPVersion = installedTP.getTechpack_version();

			String installedBuild = activatedVersionID.substring(activatedVersionID.indexOf(":") + 1);

			installedBuild = getBuildnumber(installedBuild);
			final Integer rstateCompResultforTP = compareRstates(tp0.rstate, installedTPVersion);

			if (installedVersioningVector.size() > 0) {
				if (rstateCompResultforTP == 0) {
					final Integer buildNumberInteger = Integer.valueOf(tp0.buildNumber);
					final Integer installedBuildNumberInteger = Integer.valueOf(installedBuild);

					if (buildNumberInteger > installedBuildNumberInteger) {
						logger.info("Older tech pack build b" + installedBuild
								+ " is installed. Tech pack will be updated to b" + tp0.buildNumber);

					} else {
						logger.info("Newer or the same version b" + installedBuild
								+ " of this tech pack already installed. This tech pack will not be installed. Skipping rest of the installation phases.");
						flag = true;
					}
				} else if (rstateCompResultforTP == 1) {
					logger.info("Older tech pack version " + installedTPVersion
							+ " is installed. Tech pack will be updated to version " + tp0.rstate + "_b"
							+ tp0.buildNumber);

				} else if (rstateCompResultforTP == 2) {
					// This tech pack is older than than the version in
					// database.
					logger.info("Newer version " + installedTPVersion
							+ " of this tech pack exist in database. This tech pack will not be installed.");
					flag = true;
				} else {
					throw new Exception("Could not compare the versions of techpacks. Installation has failed.");
				}
			}
		} else {
			logger.info("Previous version of techpack - " + techPackName
					+ " not found  in the system. Hence adding to the installation list");

		}

		return flag;
	}

	/**
	 * @param techPackName
	 * @param tp0
	 * @param datainterfaceSet
	 * @throws Exception
	 */
	private boolean checkIntfAlreadyInstalled(String techPackName, TPEntry tp0,
			final Vector<Datainterface> datainterfaceSet) throws Exception {
		boolean flag = false;
		if (datainterfaceSet.size() > 0) {
			logger.info("Entered checkIntfAlreadyInstalled");
			// Found activated version of this Interface
			String installedIntfRstate = datainterfaceSet.get(0).getRstate();
			String installedIntfBuild = datainterfaceSet.get(0).getInterfaceversion();
			installedIntfBuild = getBuildnumber(installedIntfBuild);

			final Integer rstateCompResultforIntf = compareRstates(tp0.rstate, installedIntfRstate);

			if (rstateCompResultforIntf == 0) {
				final Integer buildNumberInteger = Integer.valueOf(tp0.buildNumber);
				final Integer installedBuildNumberInteger = Integer.valueOf(installedIntfBuild);

				if (buildNumberInteger > installedBuildNumberInteger) {
					logger.info("Older tech pack " + techPackName + " build b" + installedIntfBuild
							+ " is installed. Tech pack will be updated to b" + tp0.buildNumber);

				} else {
					logger.info("Newer or the same version b" + installedIntfBuild + " of this tech pack "
							+ techPackName
							+ " already installed. Tech pack will not be installed. Skipping rest of the installation phases.");
					flag = true;
				}
			} else if (rstateCompResultforIntf == 1) {
				logger.info("Older tech pack version " + installedIntfRstate + " is installed. Tech pack "
						+ techPackName + " will be updated to version " + tp0.rstate + "_b" + tp0.buildNumber);

			} else if (rstateCompResultforIntf == 2) {
				// This tech pack is older than than the version in
				// database.
				logger.info("Newer version " + installedIntfRstate + " of  tech pack " + techPackName
						+ " exist in database. Tech pack will not be installed.");
				flag = true;
			} else {
				throw new Exception("Could not compare the versions of techpacks. Installation has failed.");
			}

		} else {
			logger.info("Previous version of Interface - " + techPackName
					+ " not found in the system. Hence adding to the installation list");

		}
		return flag;
	}

	/**
	 * @param rstate
	 * @param installedTPVersion
	 * @return
	 */
	private Integer compareRstates(String firstRstate, String secondRstate) {
		// TODO Auto-generated method stub
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
	 * @param installedBuild
	 * @return
	 */
	private String getBuildnumber(String installedBuild) {
		if (installedBuild.startsWith("((")) {
			installedBuild = installedBuild.substring(2);
		}

		if (installedBuild.endsWith("))")) {
			installedBuild = installedBuild.substring(0, installedBuild.length() - 2);
		}
		return installedBuild;
	}

	/**
	 * This function creates the RockFactory to dwhrep. The created RockFactory is
	 * inserted in class variable dwhrepRockFactory.
	 * 
	 * @throws Exception
	 */
	/*
	 * private void createDwhrepRockFactory() throws Exception { try { final
	 * Meta_databases whereMetaDatabases = new
	 * Meta_databases(this.etlrepRockFactory);
	 * whereMetaDatabases.setConnection_name("dwhrep");
	 * whereMetaDatabases.setType_name("USER"); final Meta_databasesFactory
	 * metaDatabasesFactory = new Meta_databasesFactory(this.etlrepRockFactory,
	 * whereMetaDatabases); final Vector<Meta_databases> metaDatabases =
	 * metaDatabasesFactory.get();
	 * 
	 * if ((metaDatabases != null) && (metaDatabases.size() == 1)) { final
	 * Meta_databases targetMetaDatabase = metaDatabases.get(0);
	 * this.dwhrepRockFactory = new
	 * RockFactory(targetMetaDatabase.getConnection_string(),
	 * targetMetaDatabase.getUsername(), targetMetaDatabase.getPassword(),
	 * etlrepRockFactory.getDriverName(), "TPInstallOrderCheck", true); } else {
	 * throw new Exception(
	 * "Unable to connect metadata (No dwhrep or multiple dwhreps defined in Meta_databases)"
	 * ); } } catch (final Exception e) { e.printStackTrace(); throw new
	 * Exception("Creating database connection to dwhrep failed.", e); } }
	 */

	/**
	 * @return
	 * @throws Exception
	 */
	/*
	 * protected Map<String, String> getDatabaseConnectionDetails() throws Exception
	 * {
	 * 
	 * ETLCServerProperties props; try { props = new ETLCServerProperties();
	 * 
	 * } catch (IOException e) { throw new
	 * Exception("Could not read ETLCServer.properties", e); }
	 * 
	 * final Map<String, String> dbConnDetails =
	 * props.getDatabaseConnectionDetails(); return dbConnDetails; }
	 * 
	 * private RockFactory createEtlrepRockFactory(final Map<String, String>
	 * databaseConnectionDetails) throws Exception { final String databaseUsername =
	 * databaseConnectionDetails.get("etlrepDatabaseUsername"); final String
	 * databasePassword = databaseConnectionDetails.get("etlrepDatabasePassword");
	 * final String databaseUrl =
	 * databaseConnectionDetails.get("etlrepDatabaseUrl"); final String
	 * databaseDriver = databaseConnectionDetails.get("etlrepDatabaseDriver");
	 * 
	 * try {
	 * 
	 * return new RockFactory(databaseUrl, databaseUsername, databasePassword,
	 * databaseDriver, "TPInstallOrderCheck", true); } catch (final Exception e) {
	 * e.printStackTrace(); throw new
	 * Exception("Unable to initialize database connection.", e); } }
	 */

	private TPEntry loadTPE(final String name, final FileInputStream versionFile) throws Exception {
		final TPEntry tpe = new TPEntry();

		final Properties p = new Properties();
		p.load(versionFile);
		tpe.name = p.getProperty("tech_pack.name");
		if (!tpe.name.equalsIgnoreCase(name)) {
			return null;
		}
		tpe.buildNumber = p.getProperty("build.number");
		tpe.rstate = p.getProperty("tech_pack.version");

		final List<String> reqs = new ArrayList<String>();

		final Enumeration keys = p.keys();
		while (keys.hasMoreElements()) {
			final String key = (String) keys.nextElement();

			if (key.startsWith("required_tech_packs.")) {
				reqs.add(key.substring(key.indexOf(".") + 1));
			}
		}

		tpe.deps = (String[]) reqs.toArray(new String[reqs.size()]);
		return tpe;
	}

	public void setTechpackDirectory(final String dir) {
		techpackDirectory = dir;
	}

	public static String getTechpackDirectory() {
		return techpackDirectory;
	}

	public File getTpDir() {
		return tpDir;
	}

	public void setTpDir(File tpDir) {
		this.tpDir = tpDir;
	}

	public String getTpName() {
		return tpName;
	}

	public void setTpName(String tpName) {
		this.tpName = tpName;
	}

	public class TPEntry {

		public String name;

		public String[] deps;

		public String filename;

		public String rstate;

		public String buildNumber;

	};

}
