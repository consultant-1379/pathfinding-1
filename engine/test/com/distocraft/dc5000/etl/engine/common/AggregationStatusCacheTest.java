package com.distocraft.dc5000.etl.engine.common;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.Statement;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.BeforeClass;
import org.junit.Test;

public class AggregationStatusCacheTest {

	static AggregationStatusCache mockedAggregationStatusCache;
	
	private static Connection con = null;
	
  @BeforeClass
  public static void setUp() throws Exception{
	  
	  try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    Statement stmt = con.createStatement();

    try {
    
    stmt.execute("create table LOG_AGGREGATIONSTATUS_ACT ( AGGREGATION varchar(31), TYPENAME varchar(31), TIMELEVEL varchar(31),"
    		+ "DATADATE date, DATE_ID date, INITIAL_AGGREGATION timestamp, STATUS varchar(31), DESCRIPTION varchar(31), "
    		+ "ROWCOUNT tinyint, AGGREGATIONSCOPE varchar(31), LAST_AGGREGATION  timestamp, LOOPCOUNT tinyint, THRESHOLD timestamp )");
    
    stmt.executeUpdate("insert into LOG_AGGREGATIONSTATUS_ACT values ( 'DC_E_RAN_DAYBH', 'DC_E_RAN', 'DAY', '1970-01-01', '2011-04-13', null,"
    		+ "'NOT_LOADED', null, null, 'DAY', null, null, null)");
    
    stmt.execute("create table LOG_AGGREGATIONSTATUS_IN ( AGGREGATION varchar(31), TYPENAME varchar(31), TIMELEVEL varchar(31),"
    		+ "DATADATE date, DATE_ID date, INITIAL_AGGREGATION timestamp, STATUS varchar(31), DESCRIPTION varchar(31), "
    		+ "ROWCOUNT tinyint, AGGREGATIONSCOPE varchar(31), LAST_AGGREGATION  timestamp, LOOPCOUNT tinyint, THRESHOLD timestamp )");

    
    }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }
    
    Mockery context = new JUnit4Mockery();
    {
        context.setImposteriser(ClassImposteriser.INSTANCE);
    }
    mockedAggregationStatusCache = context.mock(AggregationStatusCache.class);
    
  }
  
  @Test
  public void testInit() throws Exception {
	  try {	  
		  mockedAggregationStatusCache.init("jdbc:hsqldb:mem:testdb", "sa", "", "com.distocraft.dc5000.etl.engine.common.AggregationStatusCache");
	  }
	  catch(Exception e) {
		 System.out.println("Exception in Init: " + e);
	  }
  }
  
  @Test
  public void testreadDatabase() throws Exception {
	  try {
		  final Method m = AggregationStatusCache.class.getDeclaredMethod("readDatabase", String.class, long.class);
		  m.setAccessible(true);
		  AggregationStatus actual = null;
		  actual = (AggregationStatus) m.invoke(mockedAggregationStatusCache, "DC_E_RAN_DAYBH", 1L);
		  assertNotNull(actual);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in readDatabase: " + e);
	  }
  }
  
  @Test
  public void testupdate() throws Exception {
	  
	  String sql = "update LOG_AGGREGATIONSTATUS_ACT set AGGREGATION='DC_E_RAN_DAYBH'";
	  try {
		mockedAggregationStatusCache.update(sql);
	  }
	  catch(Exception e) {
			 System.out.println("Exception in update: " + e);
	  }
  }
 
  @Test
  public void testgetStatus() throws Exception {
	  try {
		AggregationStatus actual = null;
		actual = mockedAggregationStatusCache.getStatus("DC_E_RAN_DAYBH", 1L);
		assertNotNull(actual);
	  }
	  catch(Exception e) {
			 System.out.println("Exception in getStatus: " + e);
	  }
  }
  
  @Test
  public void testlogStatistics() throws Exception {
	  try {
		mockedAggregationStatusCache.logStatistics();
	  }
	  catch(Exception e) {
			 System.out.println("Exception in logStatistics: " + e);
	  }
  }
  
  @Test
  public void testReadnWriteDatabase() throws Exception {
	  try {
		  final Method mr = AggregationStatusCache.class.getDeclaredMethod("readDatabase", String.class, long.class);
		  mr.setAccessible(true);
		  AggregationStatus actual = null;
		  actual = (AggregationStatus) mr.invoke(mockedAggregationStatusCache, "DC_E_RAN_DAYBH", 1L);
		
		  final Method mw = AggregationStatusCache.class.getDeclaredMethod("writeDatabase", AggregationStatus.class);
		  mw.setAccessible(true);
		  mw.invoke(mockedAggregationStatusCache, actual);
		  
	  }
	  catch(Exception e) {
		 System.out.println("Exception in ReadnWriteDatabase: " + e);
	  }
  }
  
}
