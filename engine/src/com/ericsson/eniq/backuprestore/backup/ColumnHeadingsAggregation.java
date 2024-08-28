package com.ericsson.eniq.backuprestore.backup;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

public class ColumnHeadingsAggregation {

	
	
	//private RockFactory dwhdb;
	private static ColumnHeadingsAggregation columnHeadings;
	private ColumnHeadingsAggregation() {
	}

	public synchronized static ColumnHeadingsAggregation getConnect() {
	    if (columnHeadings == null) {
	    	columnHeadings = new ColumnHeadingsAggregation();
	    }
	    
	    return columnHeadings;
	}
	 
	
	synchronized String columnHeadersCreation(Connection dwhrepConn,String filename,String typeName,String timeLevel,Logger log)
	{
		
		String query="select dataname from dwhcolumn where storageid like'" + typeName + ":"+timeLevel+"'";
		log.finest("Column header query::"+query);
		
		ResultSet rs=null;
		BufferedWriter out = null;
		FileWriter fstream=null;
		String insert=null;
		
		StringBuilder insertstring=new StringBuilder();
		
		try {log.finest("enter inside try column test dwhrep:"+dwhrepConn+" table"+typeName+" timelevel:"+timeLevel);
			
		
			 
			 rs = dwhrepConn.createStatement().executeQuery(query);
			 log.finest("enter inside after resultset column test  table"+typeName+" timelevel:"+timeLevel);
			 
			fstream = new FileWriter(filename);
			 log.finest("enter file name for test in column"+filename);
			out = new BufferedWriter(fstream);
			while (rs.next()) {
				
				insertstring.append(rs.getString(1));
				insertstring.append(",");
				
				}
			log.finest("enter after while column test");
			insert=insertstring.substring(0,insertstring.length()-1);
			log.finest("column headers for table "+typeName+"_"+timeLevel+" is::"+insert);
			
		
				out.write(insert);
				log.finest("successfully inserted column headers for table "+typeName+"_"+timeLevel+" is::"+insert);
				out.write("\n");
			log.info("end of try");
			
		} catch (SQLException e) {
			log.warning("SQL Exception caught while fetching column names from dwhcolumn for and table name:"+typeName+"_"+timeLevel+"::"+ e);
		} 
		
		catch (IOException e) {
			log.warning("Exception caught while writing headers into text files with name "+filename+ " and table name:"+typeName+"_"+timeLevel+"::"+ e);
		}catch (Exception e) {
			log.info("result set value:"+rs);
			log.warning("Exception caught in columnHeadersCreation for "+filename+ " and table name:"+typeName+"_"+timeLevel+"::"+ e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					log.warning("Could not close BufferdWriter" + e.getMessage());
				}
			}
				try{
					if(rs != null)
					rs.close();
				}
				catch(Exception e){
					log.warning("Could not close ResultSet" + e.getMessage());
				}
				
				
		}
	log.info("before return:"+insert);
	return insert;	
	}
	
	

}
