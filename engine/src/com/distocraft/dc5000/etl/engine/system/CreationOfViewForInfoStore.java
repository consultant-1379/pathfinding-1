package com.distocraft.dc5000.etl.engine.system;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.ericsson.eniq.common.DatabaseConnections;

public class CreationOfViewForInfoStore extends TransferActionBase {
	Logger log = null;
	RockFactory dwhrep_rf = null;
	RockFactory dwhdb_rf = null;
	Connection conn_dwhrepdb = null;
	Connection conn_dwhdb = null;
	Statement stmt = null;
	ResultSet rs = null;
	Boolean toCreateview = false;
	HashMap<String, String> viewsDwhrep  = null; 
	HashMap<String,String> viewDefinitionSysView = null;
	HashMap<String,String> viewsToBeCreated = null;

	public CreationOfViewForInfoStore(final Logger parentlog) {
		log = Logger.getLogger( parentlog.getName() + "CreationOfViewForInfoStore");
		log.log(Level.INFO, "The starting timeStamp of the set is " + new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime()));
		createView();
	}

	private void createView() {
		try{
			createDBConnections(); //Creating rep and dwh DB Connections
			toCreateview = checkInfoStoreTPinstalled(); // check if the Info Store TP is installed
			if (toCreateview) { 
				log.info("View Creation will be continued since Information Store Feature is installed on the server.. ");
				frameViewDefinitionFromDB();  //Frame the view definition from the table contents in dwhrep Step 1
				viewDefFromSysViews();  //Retrieve the view definition from Sysviews if exist Step 2
				checkBothViewsareSame(viewsDwhrep , viewDefinitionSysView ); // check if both the view definitions are same(Step 1 & Step 2) if same exit else drop and recreate
				reCreateView(); // check and recreate 
			} else {
				log.log(Level.INFO, "View Creation Will not be done since the Information Store Feature(cv_information_Store Techpack) is not installed.");
			}
		}catch(Exception ex){
			log.severe("Exception occured while creating the view "+ex.getMessage());
		}finally {
			try {
				if(conn_dwhrepdb != null )
					conn_dwhrepdb.close();
				if(conn_dwhdb != null )
					conn_dwhdb.close();
				dwhdb_rf = null;
				dwhrep_rf = null;
				if(dwhdb_rf == null )
					log.info("DWH DB connections has been successfully closed");
				if(dwhrep_rf == null )
					log.info("Rep DB Connections have been successfully Closed.");
			} catch (Exception e) {
			 log.log(Level.SEVERE, "Exception while closing connections in createview()"+e.getMessage());
			}
		}
	}

	private Boolean checkInfoStoreTPinstalled() {
		conn_dwhrepdb = dwhrep_rf.getConnection();
		String techpackName = "";
		Boolean toStartViewCreation = false;
		try {
			stmt = conn_dwhrepdb.createStatement();
			rs = stmt.executeQuery("SELECT DISTINCT(TECHPACK_NAME) from TPACTIVATION WHERE TECHPACK_NAME ='CV_INFORMATION_STORE' AND STATUS ='ACTIVE'");
			log.fine("Query to check if the CV_INFORMATION_STORE techpack installed : SELECT DISTINCT(TECHPACK_NAME) from TPACTIVATION WHERE TECHPACK_NAME ='CV_INFORMATION_STORE' AND STATUS ='ACTIVE'");
			while (rs.next()) {
				techpackName = rs.getString("TECHPACK_NAME");
			}
			if (techpackName.equalsIgnoreCase("CV_INFORMATION_STORE")) {
				toStartViewCreation = true;
			}
		} catch (SQLException sqlE) {
			log.log(Level.SEVERE, "Error occured while checking the TP is active from TPActivation Table"+ sqlE.getMessage());
		}finally {
			try {
				if(rs != null){
					rs.close();
				}
			} catch (SQLException e) {
			 log.log(Level.SEVERE, "Exception while closing resultset in checkInfoStoreTPinstalled()"+e.getMessage());
			}
		}
		
		return toStartViewCreation;
	}

	private void frameViewDefinitionFromDB() {
		try {
			conn_dwhrepdb = dwhrep_rf.getConnection();
			stmt = conn_dwhrepdb.createStatement();
			rs = stmt.executeQuery("SELECT INFO.VIEW_NAME, INFO.TABLE_NAME, INFO.TECHPACK_NAME, INFO.VIEW_DEFINITION, INFO.TECHPACK_DEFINITION, INFO.CONDITIONS FROM InformationStoreViews INFO, TPACTIVATION TPA WHERE INFO.TECHPACK_NAME = TPA.TECHPACK_NAME AND TPA.STATUS = 'ACTIVE'");
			log.fine("Query to extract all the information from DWHDB: SELECT INFO.VIEW_NAME, INFO.TABLE_NAME, INFO.TECHPACK_NAME, INFO.VIEW_DEFINITION, INFO.TECHPACK_DEFINITION, INFO.CONDITIONS FROM InformationStoreViews INFO, TPACTIVATION TPA WHERE INFO.TECHPACK_NAME = TPA.TECHPACK_NAME AND TPA.STATUS = 'ACTIVE'  ");
			while(rs.next()){
				String viewName = rs.getString("VIEW_NAME")+"::"+rs.getString("VIEW_DEFINITION");
				String viewDef = "SELECT " + rs.getString("TECHPACK_DEFINITION") + " FROM " + rs.getString("TABLE_NAME")  +"  "+ (rs.getString("CONDITIONS") == null ? " "  : rs.getString("CONDITIONS")  ) ;
				if(viewsDwhrep == null){
					viewsDwhrep = new HashMap<String , String>();
					viewsDwhrep.put(viewName, viewDef);
				}else if(viewsDwhrep.containsKey(viewName)){	
					String def = viewsDwhrep.get(viewName)+"  UNION ALL "+viewDef;	
					viewsDwhrep.put(viewName, def);
				}else{
					viewsDwhrep.put(viewName, viewDef);
				}				
			}
			log.fine("Map that contains all the views to be recreated "+viewsDwhrep.size()+"  "+viewsDwhrep);
		}catch(SQLException sqlE){
			log.severe("Error Occured while checking for the view definition "+sqlE.getMessage());
		}finally {
			try {
				if(rs != null){
					rs.close();
				}
			} catch (SQLException e) {
			 log.log(Level.SEVERE, "Exception while closing resultset in frameViewDefinition() "+e.getMessage());
			}
		}	
	}
	
	private void viewDefFromSysViews(){
		conn_dwhdb = dwhdb_rf.getConnection();
		viewDefinitionSysView = new HashMap<String, String>();
	    try {
	    	for(String viewname : viewsDwhrep.keySet()){ 
	    		String view = viewname.split("::")[0];
	    		String viewdef = "IF (SELECT count(*) FROM sys.sysviews where viewname = '" + view +"') > 0 select viewtext from  sys.sysviews where viewname = '"+view+"'" ;
	    		log.fine("Query to be executed for retrieving the view definition for the "+view+" is :  "+viewdef);
	    		stmt = conn_dwhdb.createStatement();			
	    		rs = stmt.executeQuery(viewdef);
	    		if(rs != null){
	    			if(rs.next()){
	    				viewDefinitionSysView.put(view, rs.getString("viewtext"));	
	    			}
	    		}
		}
	    }catch(SQLException sqle){
	    	log.severe("Error Occured while chceking for the view definition in sysviews "+sqle.getMessage());
	    }
	    
	}
	
	private void checkBothViewsareSame(HashMap<String, String> viewFromDwh, HashMap<String,String> viewFromSys){
		try{
			HashMap<String, Set<String>> tablesFromDwh  = new HashMap<String, Set<String>>();
			HashMap<String, Set<String>> tablesFromSys  = new HashMap<String, Set<String>>();
			for (String view : viewFromDwh.keySet()){
				String def = viewFromDwh.get(view);
				Pattern p = Pattern.compile("((DIM|DC)_(E|CV)_.*?_.*?) ");
				Matcher m = p.matcher(def);
				Set tableDwh = new TreeSet<String>();
				while(m.find()){
					tableDwh.add(m.group(1).trim());	
				}
				tablesFromDwh.put(view, tableDwh);
			}for (String view : viewFromSys.keySet()){
				String def = viewFromSys.get(view);
				Pattern p = Pattern.compile("\"dc\".\"((DIM|DC)_(E|CV)_.*?_.*?)\"");
				Matcher m = p.matcher(def);
				Set tableSys = new TreeSet<String>();
				while(m.find()){
					String temp1 = m.group(1);
					tableSys.add(temp1.trim());	
				}
				tablesFromSys.put(view, tableSys);
			}
			for(String viewname : tablesFromDwh.keySet()){
				String view = viewname.split("::")[0];
				if(tablesFromSys.containsKey(view)){
					Set tempdwh  = tablesFromDwh.get(viewname);
					Set tempsys =  tablesFromSys.get(view);
					if(tempsys.containsAll(tempdwh)){
					log.info("Both the view definition(Sysviews and InformationStoreViews) are same... Hence recreation will not continue for viewname "+view);
					viewsDwhrep.remove(viewname);
					}
				}
			}
		}catch(Exception ex){
		log.severe("Error Occured while comparing both the view definition "+ex.getMessage());
		}
	}
	
	private void reCreateView(){
		try {
			conn_dwhdb = dwhdb_rf.getConnection();
			log.info("Number of views to be recreated/created are "+viewsDwhrep.size());
			for(String viewname : viewsDwhrep.keySet()){ 
				String view = viewname.split("::")[0];
				String viewColumns = viewname.split("::")[1];
				String dropCommand = "IF (SELECT count(*) FROM sys.sysviews where viewname = '" + view +"') > 0 DROP VIEW "+view ;
				String viewQuery = dropCommand+" BEGIN CREATE VIEW "+view +" ( "+viewColumns+" ) as  "+ viewsDwhrep.get(viewname)+ " END ";
				log.info("Query to be executed for recreating/Creating the "+view+" is :  "+viewQuery);
				stmt = conn_dwhdb.createStatement();			
				Boolean status = stmt.execute(viewQuery);
				if(!status){
					log.info("View "+view+" has been successfully recreated/created");
				}
			}		
		}catch(SQLException sqlE){
			log.severe("Error Occured while chceking whether the view is existing "+sqlE.getMessage());
		}
	}
	
	private void createDBConnections(){
		
		dwhdb_rf = DatabaseConnections.getDwhDBConnection();
		dwhrep_rf = DatabaseConnections.getDwhRepConnection();
		log.finest("Database connection has been established..");
		
	}
}


