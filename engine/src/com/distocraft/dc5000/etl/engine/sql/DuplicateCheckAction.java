package com.distocraft.dc5000.etl.engine.sql;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.ericsson.eniq.common.VelocityPool;

/**
 * This action checks if duplicate entries exist in database tables. This action
 * is intended to be used after Loader action. <br />
 * Copyright Distocraft 2006
 * 
 * @author Berggren
 * 
 */
public class DuplicateCheckAction extends SQLOperation {

	private Connection dwhRockFactoryConnection = null;

	protected SetContext setContext;

	protected RockFactory etlrepRockFactory;

	protected RockFactory dwhRockFactory;

	protected RockFactory dwhrepRockFactory;

	private Logger log;
	
	protected Meta_transfer_actions duplicateCheckMetaTransferAction = null;

	/**
	 * Class constructor. Initializes class variables.
	 * 
	 * @param metaVersions metaVersiond
	 * @param collectionSetId COLLECTION_SET_ID
	 * @param collection COLLECTION_ID
	 * @param transferActionId TRANSFER_ACTION_ID
	 * @param transferBatchId TRANSFER_BATCH_ID
	 * @param connectId CONNECTION_ID
	 * @param rockFactory
	 *          Database connection rockfactory-object.
	 * @param connectionPool connection pool
	 * @param metaTransferActions Meta_transfer_action
	 * @param setContext
	 *          Velocitycontext with variables set by loader.
	 * @throws EngineMetaDataException
	 */
	public DuplicateCheckAction(final Meta_versions metaVersions, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFactory, final ConnectionPool connectionPool,
			final Meta_transfer_actions metaTransferActions, final SetContext setContext) throws EngineMetaDataException {
		
		super(metaVersions, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFactory,
				connectionPool, metaTransferActions);

		this.duplicateCheckMetaTransferAction = metaTransferActions;
		this.setContext = setContext;
		this.etlrepRockFactory = rockFactory;

		try {

			final Meta_collection_sets whereCollSet = new Meta_collection_sets(this.etlrepRockFactory);
			whereCollSet.setEnabled_flag("Y");
			whereCollSet.setCollection_set_id(collectionSetId);
			final Meta_collection_sets collSet = new Meta_collection_sets(this.etlrepRockFactory, whereCollSet);

			final String techPack = collSet.getCollection_set_name();
			final String setType = collection.getSettype();
			final String setName = collection.getCollection_name();
			final String logName = techPack + "." + setType + "." + setName;
			this.log = Logger.getLogger("etl." + logName + ".loader.DuplicateCheckAction");

		} catch (Exception e) {
			throw new EngineMetaDataException("DuplicateCheckAction initialization error", e, "init");
		}
	}

	/**
	 * Executes the duplicate checking functionality.
	 */
	public void execute() throws EngineException {

		log.fine("Starting DuplicateCheckAction.");
		
		this.createDwhRockFactories();

		if (!this.setContext.containsKey("tableList")) {
			log.severe("Velocitycontext parameter \"tableList\" set by loader was not found in DuplicateCheckAction. No tables to check for, exiting duplicate checking...");

			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, new Exception(), this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		}

		String template = this.duplicateCheckMetaTransferAction.getAction_contents();

		if (template == null || template.length() <= 0) {
			template = StaticProperties.getProperty("DuplicateCheck.default", null);
		}

		if (template == null) {
			throw new EngineException(EngineConstants.CANNOT_EXECUTE + " DuplicateCheck.default is missing", null, this, this
					.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}

		Connection dwhrepRockFactoryConnection = null;

		try {
			final List<String> tableList = (List<String>) this.setContext.get("tableList");
			
			if (tableList.size() == 0) {
				log.info("List tableList in setContext was empty.");
			}

			this.dwhRockFactoryConnection = this.dwhRockFactory.getConnection();
			dwhrepRockFactoryConnection = this.dwhrepRockFactory.getConnection();

			final long startTime = System.currentTimeMillis();
			log.finest("Duplicate Start Action start time:" + startTime);
			
			for (String rawTableName : tableList) {
				// The tablename in tablelist has number postfix on it. For example
				// MY_EXAMPLE_TABLE_RAW_01.

				// Drop the postfix "_01" from the raw table name.
				final String tableName = rawTableName.substring(0, rawTableName.lastIndexOf("_"));
				final Dwhtype whereDwhType = new Dwhtype(this.dwhrepRockFactory);
				whereDwhType.setBasetablename(tableName);
				final DwhtypeFactory dwhTypeFactory = new DwhtypeFactory(this.dwhrepRockFactory, whereDwhType);
				final List<Dwhtype> dwhTypeVector = dwhTypeFactory.get();

				if (dwhTypeVector.size() == 1) {

					final Dwhtype targetDwhType = dwhTypeVector.get(0);
					final String storageId = targetDwhType.getStorageid();

					// Iterate through all the DWHColumns and get the table columns.
					final Dwhcolumn whereDwhColumn = new Dwhcolumn(this.dwhrepRockFactory);
					whereDwhColumn.setStorageid(storageId);
					whereDwhColumn.setUniquekey(1);
					final DwhcolumnFactory dwhColumnFactory = new DwhcolumnFactory(this.dwhrepRockFactory, whereDwhColumn);
					
					final List<Dwhcolumn> columns = new Vector<Dwhcolumn>();

          for (Dwhcolumn currentDwhColumn : dwhColumnFactory.get()) {
            /*log.fine(currentDwhColumn.getDataname());
                  columns.add(currentDwhColumn);*/

            //Start code changes for TR HO25609
            if (currentDwhColumn.getDataname().equals("DATETIME_ID")) {
              log.info("Removing the DATETIME_ID condition");
            } else {
              //End code changes for TR HO25609
              log.fine(currentDwhColumn.getDataname());
              columns.add(currentDwhColumn);
            }
          }

					// Mark the duplicates to the table.

					markDuplicates(rawTableName, columns, template);

				} else if (dwhTypeVector.size() == 0) {
					log.warning("DuplicateCheckAction did not found any entries from DWHType table for tablename " + tableName);
				} else {
					log.warning("DuplicateCheckAction found too many entries from DWHType table for tablename " + tableName);
				}

			}

			final long totalTime = System.currentTimeMillis() - startTime;
			log.finest("Duplicate Action Total time:" + totalTime);

		} catch (Exception e) {
			log.log(Level.SEVERE, "DuplicateCheckAction failed.", e);
			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		} finally {
			if (this.dwhRockFactoryConnection != null) {
				try {
					this.dwhRockFactoryConnection.close();
				} catch (Exception e) {
					//log.warning("dwhConnection could not be closed. " + e);
				}
			}

			if (dwhrepRockFactoryConnection != null) {
				try {
					dwhrepRockFactoryConnection.close();
				} catch (Exception e) {
					//log.warning("dwhRepConnection could not be closed. " + e);
				}
			}
		}
	}

	/**
	 * This function creates a RockFactories to dwh and dwhrep database schemas.
	 * Class variable etlrepRockFactory must be set before calling this function.
	 */
  private void createDwhRockFactories() {
    try {
      log.log(Level.FINE, "Starting to create database connections for " + getTransferActionName());
      getDwhdbConnection();
      getDwhrepConnection();
      log.fine("Database connections created successfully for " + getTransferActionName());
    } catch (Exception e) {
      log.log(Level.SEVERE, "Creation of RockFactories in DuplicateCheckAction " + getTransferActionName() + " failed.", e);
    }
  }

  private void getDwhrepConnection() throws RockException, SQLException, EngineException {
    log.fine("Trying to create dwhrep database connection for " + getTransferActionName());
    final Meta_databases where_dwhrep = new Meta_databases(etlrepRockFactory);
    where_dwhrep.setType_name("USER");
    where_dwhrep.setConnection_name("dwhrep");
    final Meta_databasesFactory fac_dwhrep = new Meta_databasesFactory(etlrepRockFactory, where_dwhrep);
    final List<Meta_databases> dwhreps = fac_dwhrep.get();
    if (dwhreps.isEmpty()) {
      throw new EngineException("No entry for CONNECTION_NAME=dwhrep found!",
        new String[]{this.getTrActions().getAction_contents()},
        null, this, getClass().getName(),
        EngineConstants.CANNOT_CREATE_DBCONNECTION);
    }
    final Meta_databases dwhrep = dwhreps.get(0);
    this.dwhrepRockFactory = new RockFactory(dwhrep.getConnection_string(),
      dwhrep.getUsername(), dwhrep.getPassword(),
      dwhrep.getDriver_name(), getTransferActionName(), true);
    log.fine("Databaseconnection to dwhrep created in DuplicateCheckActio for " + getTransferActionName());
  }

  private void getDwhdbConnection() throws EngineException, RockException, SQLException {
    log.fine("Trying to create dwh database connection for " + getTransferActionName());
    final Meta_databases where_reader = new Meta_databases(etlrepRockFactory);
    where_reader.setType_name("USER");
    /* Use getConnectionId() as this is the mapped value in the case of a multi-reader system */
    where_reader.setConnection_id(getConnectionId());
    final Meta_databasesFactory fac_reader = new Meta_databasesFactory(etlrepRockFactory, where_reader);
    final List<Meta_databases> readers = fac_reader.get();
    if (readers.isEmpty()) {
      throw new EngineException("No entry for CONNECTION_ID=" + getConnectionId() + " TYPE_NAME=USER found!",
        new String[]{this.getTrActions().getAction_contents()},
        null, this, getClass().getName(),
        EngineConstants.CANNOT_CREATE_DBCONNECTION);
    }
    final Meta_databases reader = readers.get(0);
    this.dwhRockFactory = new RockFactory(reader.getConnection_string(),
      reader.getUsername(), reader.getPassword(),
      reader.getDriver_name(), getTransferActionName(), true);
    log.fine("Databaseconnection to dwh created in DuplicateCheckAction for " + getTransferActionName());
  }

	/**
	 * This function marks the duplicates to the dwh table.
	 * 
	 * @param rawTableName
	 *          is the name of the table to be updated.
	 * @param columns
	 *          is a Vector containing DWHColumn RockObjects. These are the
	 *          columns of the table to be updated.
	 */
	protected void markDuplicates(final String rawTableName, final List<Dwhcolumn> columns, final String template)
			throws EngineException {

		VelocityEngine velocityEngine = null;

		Statement statement = null;

		try {
			log.fine("Marking duplicates for " + rawTableName);

			dwhRockFactoryConnection = this.dwhRockFactory.getConnection();
			
			if (dwhRockFactoryConnection == null) {
				log.severe("Variable connection was null in DuplicateCheckAction.markDuplicates.");
			}
					
			statement = dwhRockFactoryConnection.createStatement();

			if (statement == null) {
				log.severe("Variable statement was null in DuplicateCheckAction.markDuplicates.");
			}
			
			int numDaysToGoBackInQuery = 1;
			
			try {
				String numDaysToGoBackInQueryAsString = StaticProperties.getProperty("daysToGoBackForDC_Z_ALARM_DuplicateCheck","1");
				numDaysToGoBackInQuery = Integer.parseInt(numDaysToGoBackInQueryAsString);
			}
			catch (NullPointerException nullPE){
				log.severe("NullPointerException in Static.Properties file.");
			}
			catch (NumberFormatException nE){
				log.severe("Static Property daysToGoBackForDC_Z_ALARM_DuplicateCheck is not set properly");
			}
			
			
			// Create the Velocity context and set it's references.
			final VelocityContext context = new VelocityContext();

			log.fine("rawTableName = " + rawTableName);
			log.fine("columns content");
			log.fine("numDaysToGoBackInQuery = " + numDaysToGoBackInQuery);
			
			context.put("rawTableName", rawTableName);
			context.put("columns", columns);
			context.put("numDaysToGoBackInQuery", numDaysToGoBackInQuery);
			
			final StringWriter output = new StringWriter();

			if (this.duplicateCheckMetaTransferAction == null) {
				log.severe("duplicateCheckMetaTransferAction was null in DuplicateCheckAction.markDuplicates.");
			}

			if (this.duplicateCheckMetaTransferAction.getAction_contents() == null) {
				log.severe("duplicateCheckMetaTransferAction.getAction_contents() was null in DuplicateCheckAction.markDuplicates.");
			}

			log.finest("duplicateCheckMetaTransferAction.getAction_contents() = "
					+ duplicateCheckMetaTransferAction.getAction_contents());

			velocityEngine = VelocityPool.reserveEngine();

			final boolean velocityEvaluateSuccesfull = velocityEngine.evaluate(context, output, "DuplicateCheckAction",
					template);

			if (!velocityEvaluateSuccesfull) {
				log.warning("DuplicateCheckAction failed to evaluate action contents.");
			}

			final String sqlQuery = output.toString();

			log.fine("Executing query: " + sqlQuery);
			statement.executeUpdate(sqlQuery);

		} catch (Exception e) {
			log.log(Level.SEVERE, "Marking duplicates in DuplicateCheckAction failed.", e);

			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		} finally {
			VelocityPool.releaseEngine(velocityEngine);
			if (statement != null) {
				try {
					statement.close();
				}
				catch (Exception e) {
					log.warning("DuplicateCheckAction Statement not closed : " + e);
					}
			}
		}
	}

}
