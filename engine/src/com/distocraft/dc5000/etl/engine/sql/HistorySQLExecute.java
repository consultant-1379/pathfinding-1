/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2011 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.sql;

import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.VelocityPool;

/**
 * HistorySQLExecute determines the Volume Based Partition to use in an sql template.
 * Partition derived from typename, needs to be provided in the set's WhereClause.
 * 
 * @author eeoidiv, 201109.
 */
public class HistorySQLExecute extends SQLOperation {

  private final transient Logger log;
  private final transient String typename;
  private final transient String clause;
  private final transient String storageId;

  public HistorySQLExecute(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
      final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog) throws Exception {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
        trActions);
    
    this.log = Logger.getLogger(clog.getName() + ".HistorySQLExecute");
    final String where = trActions.getWhere_clause();
    final Properties prop = stringToProperties(where);
    typename = prop.getProperty("typename");
    log.finer("HistorySQLExecute typename: " + typename);
    storageId = typename+"_HIST:RAW";//TODO: Improve
	log.finer("HistorySQLExecute storageId: " + storageId);
    clause = trActions.getAction_contents();
    log.finer("HistorySQLExecute SQLclause: " + clause);
  } //HistorySQLExecute

  @Override
  public void execute() throws Exception {
    VelocityEngine velocityEngine = null;
    try {
      final VelocityContext context = new VelocityContext();
      velocityEngine = VelocityPool.reserveEngine();
      try {
    	fillVelocityContext(context);
	    final StringWriter writer = new StringWriter();
	    velocityEngine.evaluate(context, writer, "", clause);
	    final String sqlClause = writer.toString();
	    log.finer("Trying to execute: " + sqlClause);
	    this.getConnection().executeSql(sqlClause);
      } catch (SQLException e) {
        log.log(Level.WARNING, "SQL execution failed to exception", e);
        throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.getTrActions()
            .getAction_contents() }, e, this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
      }
    } finally {
      if (velocityEngine != null) {
        VelocityPool.releaseEngine(velocityEngine);
      }
    }
  } // execute
  
  protected void fillVelocityContext(final VelocityContext context) {
		context.put("typename", typename);
  } //fillVelocityContext

  /**
   * Determine the current Partition to use, based on row count Volume based partition logic.
   * @return
   */
  protected String getPartition(final String storageID) {
	  String partition = null;
	  try {
		  final List<PhysicalTableCache.PTableEntry> partitions = getListOfPartitions(storageID);
		  PhysicalTableCache.PTableEntry table = null;
		  long rowcount = -1;
		  //Want the first non-full partition OR if all full then the lowest loadOrder.  
		  for (int partitionNo = 0; partitionNo < partitions.size(); partitionNo++) {
			  table = partitions.get(partitionNo);
			  if (table.loadOrder == null || table.loadOrder == 0) {
				  updatePartitionOrder(storageID, table);
			  }
			  rowcount = getTableRowCount(table.tableName);		
			  log.fine("partition: " + table.tableName+ " #" + table.loadOrder + " " + rowcount + "/" + table.partitionsize);
			  if (rowcount < table.partitionsize) {
				  log.fine("Using partition: " + table.tableName+ " #" + table.loadOrder + " " + rowcount + "/" + table.partitionsize);
				  break;
			  }
		  }//while(partitionNo<partitions.size())
		  partition =  table.tableName;// The last table in order, if none selected by break in loop.
		  //Truncate the partition, if it was full. We are going round-robin, full because oldest.
		  if (rowcount >= table.partitionsize) {
			  try {
				  truncateTable(partition);
				  //Increment partition's loadOrder, so it will no longer be the lowest loadOrder, therefore the next full partition will be emptied.
				  updatePartitionOrder(storageID, table);
			  } catch (SQLException e) {
				  log.log(Level.WARNING, "Error while truncating Partition to be used:"+partition,e);
			  }
		  }
	  } catch (Exception e) {
		  log.log(Level.WARNING, "Error while determining Partition to be used, defaulting to:"+typename+"_HIST_RAW_01",e);
		  partition =  typename+"_HIST_RAW_01";
	  }
	  return partition;
  } // getPartition

	/**
	 * @param storageID
	 * @param table
	 * @throws RockException
	 * @throws SQLException
	 */
	private void updatePartitionOrder(final String storageID,
			PhysicalTableCache.PTableEntry table) throws RockException,
			SQLException {
		  Integer newLoadOrder = getPartitionsMaxLoadOrder(storageID) + 1;
		  table.loadOrder = newLoadOrder;
		  PhysicalTableCache.getCache().updateLoadOrder(storageID, table.tableName, newLoadOrder);
		  log.fine("Assigning loadOrder "+newLoadOrder+" to table " + table.tableName);
	}//updatePartitionOrder
  
  /**
   * Truncate a table. The current transaction will be implicitly committed.
   * 
   * @param tableName
   * @return
   * @throws SQLException
   *           - if truncation fails or throws an error.
   */
  protected void truncateTable(final String tableName) throws SQLException {
  	final RockFactory dwhRock = getConnection();
  	
    Statement truncateTable = null;
    try {
      truncateTable = dwhRock.getConnection().createStatement();
      truncateTable.executeUpdate("TRUNCATE TABLE " + tableName);
      log.info("Truncated partition " + tableName);
    } finally {
      try {
        if (truncateTable != null) {
          truncateTable.close();
        }
      } catch (final SQLException e) {
      	log.log(Level.FINE, "Cleanup failed",e);
      }
    }
  }//truncateTable
  
  /**
   * Find the current row count for a partition table.
   * 
   * @param tableName
   * @return the number of table rows, or null if the target table is unknown
   * @throws IllegalStateException
   *           if target table is not present
   */
  protected Long getTableRowCount(final String tableName) throws SQLException, IllegalStateException {
  	final RockFactory dwhRock = getConnection();
  	
  	Long rowCount = null;
    Statement stmt = null;
    ResultSet result = null;
    
    try {
    	stmt = dwhRock.getConnection().createStatement();
      result = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
      if (result.next()) {
        rowCount = result.getLong(1);
      } else {
        throw new IllegalStateException("Attempt to access a non-existant table:" + tableName);
      }

      return rowCount;
    } finally {
      try {
        if (result != null) {
          result.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      } catch (final SQLException e) {
        log.log(Level.FINE, "Cleanup failed",e);
      }
    }
  }//getTableRowCount
  
  /**
   * Gets list of partitions for loading. Partitions are queried from PhysicalTableCache and ordered to loading order. 
   */
	protected List<PhysicalTableCache.PTableEntry> getListOfPartitions(final String storageID) throws RockException {
		final PhysicalTableCache ptc = PhysicalTableCache.getCache();
		final List<PhysicalTableCache.PTableEntry> partitions = ptc.getEntries(storageID);
		if(partitions == null || partitions.size() <= 0) {
			throw new RockException("No tables for type " + storageID);
		} else {
			log.fine(partitions.size() + " partitions found for storageID \"" + storageID + "\"");
		}
		//Sorts by loadOrder or else by name. If by name, will return last named Partition first (this is ok). 
		Collections.sort(partitions);
		return partitions;
	}//getListOfPartitions
	
	/**
	 * From the list of partitions, find the highest loadOrder.
	 * If a partitions loadOrder is null, treat as zero.
	 * @return
	 */
	protected int getPartitionsMaxLoadOrder(final String storageID) {
		int max = 0;
		int currentNumber = -1;
		try {
			final List<PhysicalTableCache.PTableEntry> partitions = getListOfPartitions(storageID);
			for (PhysicalTableCache.PTableEntry table : partitions) {
				if(table.loadOrder==null) {
					currentNumber = 0;
				} else {
					currentNumber = table.loadOrder;
				}
				if(currentNumber>max) {
					max = currentNumber;
				}
			}
		} catch (Exception e) {
			max = 0;
		}
		return max;
	}//getPartitionsMaxLoadOrder

}// class HistorySQLExecute
