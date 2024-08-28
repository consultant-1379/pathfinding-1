package com.ericsson.eniq.Services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

//import org.apache.tools.ant.Exception;
//import org.apache.tools.ant.Task;

import ssc.rockfactory.RockFactory;


/**
 * A simple Ant task implementation to update the dataitem table in the dwhrep 
 * with values for datatype, datasize and datascale columns from measurementcolumn 
 * and referencecolumn tables.
 *  
 * @author esunbal
 * @see org.apache.tools.ant.Task
 *
 */
public class UpdateDataItem{
	
	private static final Logger logger = LogManager.getLogger(UpdateDataItem.class);
	
	private String currentWorkingDirectory = "";		

	private final String updateSqlFile = "update.sql";

	private final double batchSize = 1000;	

	private transient RockFactory etlrepRockFactory = null;

	private transient RockFactory dwhrepRockFactory = null;

	private final String selectQuery = "select distinct di.dataformatid, di.dataname, mc.datatype, mc.datasize, mc.datascale " +
	"from measurementcolumn mc, dataitem di " +
	"where di.dataname = mc.dataname " +
	"and substring(di.dataformatid,1,length(di.dataformatid)-strpos(':',reverse(di.dataformatid))) = substring(mc.mtableid,1,length(mc.mtableid)-strpos(':',reverse(mc.mtableid))) " +
	"and di.datatype = null " +
	"union " +
	"select distinct di.dataformatid, di.dataname, rc.datatype, rc.datasize, rc.datascale " +
	"from referencecolumn rc, dataitem di " +
	"where di.dataname = rc.dataname " +
	"and substring(di.dataformatid,1,length(di.dataformatid)-strpos(':',reverse(di.dataformatid))) = rc.typeid " +
	"and di.datatype = null";

	/* 
	 * The entry method for this class.
	 * @see org.apache.tools.ant.Task#execute()	 * 
	 */
	
	public UpdateDataItem(String tpDirectory)
	{
		this.currentWorkingDirectory=tpDirectory;
	}

	public void execute() throws Exception {
		/*
		 * Tasks:
		 * 0.) Sanity checks.
		 * 1.) Get the database connection
		 * 2.) Run the select query to create the file with list of update statements
		 * 3.) Read the file, create batches of 1000 and execute the update
		 * 4.) Remove the temporary file update.sql
		 */
	
		
		// Task 0: Sanity Checks
		

		logger.info("Inside execute method of DataItem");

		if (!this.currentWorkingDirectory.endsWith(File.separator)) {
			this.currentWorkingDirectory = this.currentWorkingDirectory
			+ File.separator;
		}
		
		logger.info(this.currentWorkingDirectory);
		
		 //Task 1: Get the database connection.
		 	
		this.createDatabaseConnections();

		
		// Task 2: Get the information from MeasurementColumn and ReferenceColumn tables.
		
		this.getDataForDataItemTable();

		
		// Task 3: Run the batch updates on DataItem table.		

		this.updateDataItem();
		
		// Task 4: Remove the temporary file update.sql
		
		this.removeUpdateSqlFile();
	}


	/**
	 * Removes the temporary update sql file. 
	 */
	private void removeUpdateSqlFile(){
		//TODO : Will be implemented when the code becomes stable.
	}

	/**
	 * Utility method to get the number of lines in a file.
	 * @param fileName
	 * @return double
	 */
	public double getNumberOfLines(String fileName) {
		double numberOfLines = 0;		
		LineNumberReader lineCounter = null;
		try {
			lineCounter = new LineNumberReader(new FileReader(new File(fileName)));
			while ((lineCounter.readLine()) != null) { 
				continue;
			}
			numberOfLines = lineCounter.getLineNumber();

		} catch (IOException e) {
			logger.info("Unable to read the file "+fileName);
			e.printStackTrace();
		}

		return numberOfLines;
	}


	/**
	 * Updates the dataitem table by running the update sqls from the update.sql file
	 * in batches of 1000 update statements.
	 */
	private void updateDataItem(){		

		Connection connection = this.dwhrepRockFactory.getConnection();	
		String sqlQuery = null;

		int batchNumber = 0;
		double rowCountPerBatch = 0;
		double rowCountInUpdateSql = 0;
		double numberOfRowsInUpdateSQLFile = getNumberOfLines(this.currentWorkingDirectory+this.updateSqlFile);
		logger.info("Number of rows in "+this.updateSqlFile+" is:"+numberOfRowsInUpdateSQLFile);
		double numberOfBatches = Math.ceil(numberOfRowsInUpdateSQLFile / batchSize);
		logger.info("Executing "+numberOfRowsInUpdateSQLFile+" sql updates in "+numberOfBatches+" batches");		

		File file = new File(this.currentWorkingDirectory+this.updateSqlFile);		
		BufferedReader br = null;
    Statement statement = null;
		try {
			br = new BufferedReader(new FileReader(file));
      statement = connection.createStatement();
			int[] updateCounts = null;	

			// Disable auto-commit	
			connection.setAutoCommit(false);
			
			while ((sqlQuery = br.readLine()) != null) {
				// Read the update.sql file in batches of this.batchSize - currently set to 1000 and add it in the batch for execution.				
				statement.addBatch(sqlQuery);				
				rowCountPerBatch++;	
				rowCountInUpdateSql++;

				if((rowCountPerBatch == batchSize) || (rowCountInUpdateSql == numberOfRowsInUpdateSQLFile)){
					// submit a batch of update commands for execution
					batchNumber++;
					updateCounts = statement.executeBatch();
					statement.clearBatch();
					rowCountPerBatch = 0;					
				}
			}	

			// Since there were no errors, commit
			connection.commit();

		}
		catch (BatchUpdateException be) {
			logger.info("Not all the statements in batch:"+batchNumber+" were successfully executed");
			
			// Get the updates for the batch and iterate the batch to see which row failed.
			int[] updateCounts = be.getUpdateCounts();
			processUpdateCounts(updateCounts, batchNumber);

		} 
		catch(SQLException se){
			BackupAndRestore bnr=new BackupAndRestore();
			  bnr.restore();
			logger.info("Exception in running a SQL statement.");
			se.printStackTrace();
		}
		catch(IOException ie){
			logger.info("Exception in file handling of update.sql");
			ie.printStackTrace();
		}
		finally{			
			try {
        if (br != null) {
          br.close();
        }
        if (statement != null) {
          statement.close();
        }
			} catch (IOException e) {
				e.printStackTrace();
      } catch (SQLException e) {
        logger.info("error closing statement");
      }
		}	
	}


	/**
	 * Creates the database connection for etlrep and dwhrep databases.
	 * @throws Exception 
	 */

	private void createDatabaseConnections() throws Exception{
		
		logger.info("Checking connection to database");
		GetDatabaseDetails getdb=new GetDatabaseDetails();
		
		final Map<String, String> databaseConnectionDetails =getdb.getDatabaseConnectionDetails();
		
		
		this.etlrepRockFactory = getdb.createEtlrepRockFactory(databaseConnectionDetails);

		
		this.dwhrepRockFactory=getdb.createDwhrepRockFactory(this.etlrepRockFactory);

		logger.info("Connections to database created.");

	}


	/**
	 * Queries the measurementcolumn and referencecolumn to populate the update.sql file 
	 * with update sqls on dataitem table.
	 */
	private void getDataForDataItemTable(){
		BufferedWriter out = null;
		try {

			String dataFormatId = null;
			String dataName = null;			
			String datatype = null;
			Integer datasize = null;
			Integer datascale = null;
			
			Connection connection = this.dwhrepRockFactory.getConnection();
			Statement statement = connection.createStatement();
			logger.info("sqlQuery:"+this.selectQuery);
			ResultSet resultSet = statement.executeQuery(this.selectQuery);
			out = new BufferedWriter(new FileWriter(this.currentWorkingDirectory+this.updateSqlFile));
			
			String updateString = null;			
			int rowCounter = 0;
			
			while(resultSet.next()){
				rowCounter++;
				dataFormatId = resultSet.getString(1);
				dataName = resultSet.getString(2);
				datatype  = resultSet.getString(3);
				datasize  = resultSet.getInt(4);
				datascale  = resultSet.getInt(5);
				updateString = "update DataItem set datatype='"+datatype+"', "+"datasize="+datasize+", "+"datascale="+datascale+" where dataformatid='"+dataFormatId+"' and dataname='"+dataName+"'";				
				out.write(updateString);
				out.write("\n");
			}
		}
		catch (IOException e) {
			// TODO Need to change this into more robust error checking mechanism.
			logger.info("Exception in File handling of update.sql.");			
			e.printStackTrace();
		}
		catch (SQLException e) {
			// TODO Need to change this into more robust error checking mechanism.
			logger.info("Exception in querying for values from the database.");
			e.printStackTrace();
		} 
		finally{
			try {
				out.close();
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}


	/**
	 * Exception handling utility method to determine which update statement failed.
	 * @param updateCounts
	 * @param batchNumber
	 */
	public void processUpdateCounts(int[] updateCounts, int batchNumber) {		
		double recordNumber = 0;
		for (int i=0; i<updateCounts.length; i++) {
			recordNumber = batchNumber * batchSize + i;
			if (updateCounts[i] == Statement.SUCCESS_NO_INFO) {
				// Successfully executed - number of affected rows not available
				logger.info("Updation of Record "+recordNumber+" succeeded but with no info on affected rows.");
			} else if (updateCounts[i] == Statement.EXECUTE_FAILED) {
				// Failed to execute				
				logger.info("Updation of Record "+recordNumber+" failed");
			}
		}
	}


	
	/**
	 * @return
	 */
	public String getCurrentWorkingDirectory() {
		return currentWorkingDirectory;
	}

	/**
	 * @param currentWorkingDirectory
	 */
	public void setCurrentWorkingDirectory(String currentWorkingDirectory) {
		this.currentWorkingDirectory = currentWorkingDirectory;
	}
}
