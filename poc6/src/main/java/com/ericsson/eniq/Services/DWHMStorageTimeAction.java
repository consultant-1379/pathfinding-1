package com.ericsson.eniq.Services;

import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;

import ssc.rockfactory.RockFactory;

public class DWHMStorageTimeAction {

	private RockFactory etlreprock = null;
	private RockFactory dwhreprock = null;
	private RockFactory dwhrock = null;
	private RockFactory dbadwhrock = null;

	private final String tpName;
	private final String tpDirectory;

	private static final Logger logger = Logger.getLogger("DWHMStorageTimeAction");

	@Autowired
	private GetDatabaseDetails databaseDetails;

	public DWHMStorageTimeAction(String tpName, String tpDirectory) {
		this.tpDirectory = tpDirectory;
		this.tpName = tpName;
	}

	public void execute() {

		try {

			GetDatabaseDetails databaseDetails = new GetDatabaseDetails();
			final Map<String, String> databaseConnectionDetails = databaseDetails.getDatabaseConnectionDetails();
			this.etlreprock = databaseDetails.createEtlrepRockFactory(databaseConnectionDetails);
			this.dwhreprock = databaseDetails.createDwhrepRockFactory(databaseConnectionDetails);
			this.dwhrock = databaseDetails.createDwhdbRockFactory(databaseConnectionDetails);
			this.dbadwhrock = databaseDetails.createDBADwhdbRockFactory(databaseConnectionDetails);

			logger.info("Connections to database created.");

			StorageTimeAction sta = new StorageTimeAction(this.dwhreprock, etlreprock, this.dwhrock, this.dbadwhrock,
					this.tpName, this.tpDirectory, logger);

		} catch (Exception e) {
			e.printStackTrace();
			try {
				throw new Exception("Unexpected failure");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			try {
				if (etlreprock != null) {
					etlreprock.getConnection().close();
				}
				if (dwhreprock != null) {
					dwhreprock.getConnection().close();
				}
				if (dwhrock != null) {
					dwhrock.getConnection().close();
				}
				if (dbadwhrock != null) {
					dbadwhrock.getConnection().close();
				}
			} catch (final SQLException sqle) {
				System.out.print("Connection cleanup error - " + sqle.toString());
			}
			dwhreprock = null;
			etlreprock = null;
			dwhrock = null;
			dbadwhrock = null;
		}

	}

}
