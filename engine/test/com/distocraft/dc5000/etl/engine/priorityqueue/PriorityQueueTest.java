package com.distocraft.dc5000.etl.engine.priorityqueue;

import static org.junit.Assert.*;

import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.Constants;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.engine.main.EngineThread;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;

public class PriorityQueueTest {

	public static ExecutionSlotProfileHandler esph;
  private static Connection testDb = null;
  private static final String AGG_STATUS_TABLE = "LOG_AggregationStatus_01";
	
	@BeforeClass
  public static void setUpBeforeClass() throws Exception {
    StaticProperties.giveProperties(new Properties());

    try {
      Class.forName("org.hsqldb.jdbcDriver").newInstance();
      testDb = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    Statement stmt = testDb.createStatement();
    stmt.execute("CREATE TABLE META_EXECUTION_SLOT_PROFILE (PROFILE_NAME VARCHAR(31), PROFILE_ID VARCHAR(31), "
        + "ACTIVE_FLAG VARCHAR(31))");
    stmt.execute("CREATE TABLE META_EXECUTION_SLOT (PROFILE_ID VARCHAR(31), SLOT_NAME VARCHAR(31), "
        + "SLOT_ID VARCHAR(31), ACCEPTED_SET_TYPES VARCHAR(31), SERVICE_NODE varchar(64))");
    stmt.executeUpdate("INSERT INTO META_EXECUTION_SLOT_PROFILE VALUES('profilename', 'profileid', 'y')");
    stmt.executeUpdate("INSERT INTO META_EXECUTION_SLOT VALUES('profileid', 'slotname', '0', 'testset', null)");
    
    stmt.execute("CREATE TABLE "+AGG_STATUS_TABLE+" (" +
      "AGGREGATION varchar(255), TYPENAME varchar(255), TIMELEVEL varchar(10)," +
      "DATADATE date, DATE_ID date, INITIAL_AGGREGATION TIMESTAMP, STATUS varchar(16), " +
      "DESCRIPTION varchar(250), ROWCOUNT integer, AGGREGATIONSCOPE varchar(50), " +
      "LAST_AGGREGATION	timestamp, LOOPCOUNT integer, THRESHOLD timestamp)");

    stmt.close();
    
    esph = new ExecutionSlotProfileHandler("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver");
      
  }
  @AfterClass
  public static void afterClass(){
    if(testDb != null){
      try {
        testDb.createStatement().execute("SHUTDOWN");
        testDb.close();
      } catch (SQLException e) {
        /*-*/
      }
    }
  }
	
	@Test
	public void testGetSetsForTableName() throws Exception {
		final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		final EngineThread set = new EngineThread("Set5", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		pq.addSet(set);
		set.addSetTable("EXAMPLE_TABLE");
		List ret = pq.getSetsForTableName("EXAMPLE_TABLE");
		assertEquals(0, ret.size());
	}
	
	 @Test
	 public void addSetHappy() throws Exception {
		 
		 StaticProperties.giveProperties(new Properties());
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 
		 final EngineThread engineSet = new EngineThread("testSet5", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("testSet10", Long.valueOf(10L), Logger.getLogger("Test"), new EngineCom());
		 engineSet2.setPersistent(true);
		 pq.addSet(engineSet2);
		
		 final EngineThread engineSet3 = new EngineThread("testSet10", Long.valueOf(10L), Logger.getLogger("Test"), new EngineCom());
		 engineSet3.setPersistent(true);
		 pq.addSet(engineSet3);
		 
		 if(pq.getNumberOfAvailable() != 3) {
			 fail("Added 3, but contains " + pq.getNumberOfAvailable());
		 }
		 
		 if(engineSet2.getPersistentID().equals(engineSet3.getPersistentID())) {
			 fail("Persistent IDs match");
		 }
		 
	 }
	 	 	 
	 @Test
	 public void addSetNull() throws Exception {
		 
		 StaticProperties.giveProperties(new Properties());
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 
		 pq.addSet(null);
		 
		 if(pq.getNumberOfAvailable() != 0) {
			 fail("Null set was inserted into queue");
		 }
		 
	 }
	 
	 @Test
	 public void addSetNullPrio() throws Exception {
		 
		 StaticProperties.giveProperties(new Properties());
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 
		 final EngineThread engineSet = new EngineThread("testSet5", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("testSet10", null, Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		
		 if(pq.getNumberOfAvailable() != 1) {
			 fail("Null prio set was inserted into queue");
		 }
		 
	 }
	 
	 @Test
	 public void addSetDublicateLoaders() throws Exception {
		 
		 final Properties props = new Properties();
		 props.setProperty("PriorityQueue.maxAmountOfLoadersForSameTypeInQueue", "1");
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 
		 final EngineThread engineSet = new EngineThread("Loader", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("Loader", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		
		 final EngineThread engineSet3 = new EngineThread("Loader", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet3);
		 
		 if(pq.getNumberOfAvailable() != 1) {
			 fail("Duplicate Loader was inserted into queue");
		 }
		 
	 }
	 
	 @Test
	 public void addSetDublicate() throws Exception {
		 
		 final Properties props = new Properties();
		 props.setProperty("PriorityQueue.DuplicateCheckedSetTypes","type1");
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 
		 final EngineThread engineSet = new EngineThread("Loader", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 final Properties schpro = new Properties();
		 schpro.setProperty("aggDate", "120000");
		 schpro.setProperty("timelevel", "1");
		 engineSet.setSchedulingInfo(TransferActionBase.propertiesToString(schpro));
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet2.setSchedulingInfo(TransferActionBase.propertiesToString(schpro));
		 pq.addSet(engineSet2);
		
		 final EngineThread engineSet3 = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet3.setSchedulingInfo(TransferActionBase.propertiesToString(schpro));
		 pq.addSet(engineSet3);
		 
		 if(pq.getNumberOfAvailable() != 2) {
			 fail("General duplicate was inserted into queue");
		 }
		 
	 }
	 
	 @Test
	 public void releaseSet() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet.setEarliestExection(new Date(System.currentTimeMillis() + 100000L));
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type2", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);

		 if(pq.getNumberOfAvailable() != 1) {
			 fail("getNumberOfAvailable should not return postponed sets");
		 }
		 
		 pq.releaseSet(engineSet.getQueueID());
	 
		 if(pq.getNumberOfAvailable() != 2) {
			 fail("Release did not release set");
		 }
		 
	 }
		 
	 @Test
	 public void releaseSets() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet.setEarliestExection(new Date(System.currentTimeMillis() + 100000L));
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type2", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet2.setEarliestExection(new Date(System.currentTimeMillis() + 100000L));
		 pq.addSet(engineSet2);

		 final EngineThread engineSet3 = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet3);
		 
		 if(pq.getNumberOfAvailable() != 1) {
			 fail("getNumberOfAvailable should not return postponed sets");
		 }
		 
		 pq.releaseSets(null, engineSet2.getSetType());
	 
		 if(pq.getNumberOfAvailable() != 2) {
			 fail("Release did not release set");
		 }
		 
	 }
	 
	 @Test
	 public void executeSet() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 
		 final EngineThread engineSet = new EngineThread("Type1", Long.valueOf(15L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("Type2", Long.valueOf(15L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		 		 
		 if(pq.getNumberOfSetsInQueue() != 2) {
			 fail("Sets did not enter queue");
		 }

		 pq.executeSet(engineSet);
		 
		 if(pq.getNumberOfSetsInQueue() != 1) {
			 fail("Execute did not remove set from queue");
		 }
		 
		 pq.executeSet(null);
		 
		 if(pq.getNumberOfSetsInQueue() != 1) {
			 fail("Execute null altered queue");
		 }
		 
	 }
	 
	 @Test
	 public void unRemoveSetHappy() throws Exception {
		 
		 final Properties props = new Properties();
		 props.setProperty("PriorityQueue.unremovableSetTypes", "type1");
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 
		 final EngineThread engineSet = new EngineThread("Type1", Long.valueOf(15L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 engineSet.setChangeDate(new Date(Long.valueOf(1000L)));
		 
		 final EngineThread engineSet2 = new EngineThread("Type2", Long.valueOf(15L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		 
		 engineSet2.setChangeDate(new Date(Long.valueOf(1000L)));
		 
		 pq.houseKeep();
		 
		 if(pq.getNumberOfSetsInQueue() != 1) {
			 fail("Unremovable set was dropped or removable was not dropped " + pq.getNumberOfSetsInQueue());
		 }
		 
	 }
	 
	 
	 @Test
	 public void removeSetHappy() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		 
		 if(pq.getNumberOfAvailable() != 2) {
			 fail("Add failed");
		 }
		 
		 pq.removeSet(engineSet2);
		 
		 if(pq.getNumberOfAvailable() != 1) {
			 fail("After removal there should be only one set in queue");
		 }
		 
	 }
		 
	 @Test
	 public void removeSetNull() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		 
		 if(pq.getNumberOfAvailable() != 2) {
			 fail("Add failed");
		 }
		 
		 pq.removeSet(null);
		 
		 if(pq.getNumberOfAvailable() != 2) {
			 fail("After removal of null set there should be two set in queue");
		 }
		 
	 }
	 
	 @Test
	 public void removeSetNonExistent() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 
		 if(pq.getNumberOfAvailable() != 1) {
			 fail("Add failed");
		 }
		 
		 pq.removeSet(engineSet2);
		 
		 if(pq.getNumberOfAvailable() != 1) {
			 fail("After removal of nonexistent set there should be one set in queue");
		 }
		 
	 }
	 
	 @Test
	 public void changePriorityHappy() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		 
		 if(pq.getNumberOfAvailable() != 2) {
			 fail("Add failed");
		 }
		 
		 if(!pq.changePriority(engineSet2, 10L)){
			 fail("changePriority failed");
		 }
		 
		 if(engineSet.getSetPriority() != 5 || engineSet2.getSetPriority() != 10) {
			 fail("Priority not changed " + engineSet.getSetPriority() + " " + engineSet2.getSetPriority());
		 }
		 
	 }
	 
	 @Test
	 public void changePriorityNull() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);

		 pq.changePriority(null, 10);
		 
	 }
	 
	 @Test
	 public void changePriorityOverMax() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 if(pq.changePriority(engineSet, 20L)){
			 fail("changePriority should fail");
		 }
		 		 
	 }
	 
	 @Test
	 public void changePriorityBelowZero() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 if(pq.changePriority(engineSet, -1L)){
			 fail("changePriority should fail");
		 }
		 		 
	 }
	 
	 @Test
	 public void getAvailableHappy() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type2", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet2.setActive(false);
		 pq.addSet(engineSet2);
		 
		 final EngineThread engineSet3 = new EngineThread("type3", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet3.setEarliestExection(new Date());
		 pq.addSet(engineSet3);
		 
		 int count = 0;
		 final Iterator<EngineThread> eti = pq.getAvailable();
		 while(eti.hasNext()) {
			 eti.next();
			 count ++;
		 }
		 
		 if(count != 1) {
			 fail("GetAvailable retuned " + count + " expecting 1");
		 }
		 
		 if(count != pq.getNumberOfAvailable()) {
			 fail("getNumberOfAvailable does not match getAvailable");
		 }
		 		 
	 }
	 
	 @Test
	 public void getAllHappy() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type2", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet2.setActive(false);
		 pq.addSet(engineSet2);
		 
		 final EngineThread engineSet3 = new EngineThread("type3", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet3.setEarliestExection(new Date());
		 pq.addSet(engineSet3);
		 
		 int count = 0;
		 final Iterator<EngineThread> eti = pq.getAll();
		 while(eti.hasNext()) {
			 eti.next();
			 count ++;
		 }
		 
		 if(count != 3) {
			 fail("GetAvailable retuned " + count + " expecting 1");
		 }
		 		 
	 }
	 
	 @Test
	 public void getFind() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type2", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		 
		 final EngineThread engineSet3 = new EngineThread("type3", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet3);

		 final EngineThread et = pq.find(engineSet.getQueueID());
		 
		 if(et != engineSet) {
			 fail("Not returning correct one");
		 }
		 		 
		 final EngineThread empty = pq.find(10000L);
		 
		 if(empty != null) {
			 fail("Returning value for nonexixtent queueid");
		 }
		 
		 final EngineThread empty2 = pq.find(10000L,10000L);
		 
		 if(empty2 != null) {
			 fail("Returning value for nonexixtent queueid");
		 }
		 
	 }


  @Test
  public void testHouseKeep_AggregationFailedDependency() throws Exception {
    final Properties props = new Properties();
    StaticProperties.giveProperties(props);
    final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
    final EngineThread set = new EngineThread("Aggregator", 15L, Logger.getLogger("Test"), new EngineCom());
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    final Date aggDate = new Date(System.currentTimeMillis() - 60000);

    final AggregationStatus aggStatus = new AggregationStatus();
    aggStatus.STATUS = Constants.AGG_FAILED_STATUS;
    aggStatus.AGGREGATION = "some_aggregation";
    aggStatus.DATADATE = aggDate.getTime();

    testDb.createStatement().executeUpdate("insert into " + AGG_STATUS_TABLE + " (AGGREGATION, DATADATE) " +
      "values('"+aggStatus.AGGREGATION+"', '"+new java.sql.Date(aggStatus.DATADATE)+"')");

    final String dbUrl = "jdbc:hsqldb:mem:testdb";
    final String driver = "org.hsqldb.jdbcDriver";
    final String SA = "SA";

    PhysicalTableCache.testInit("LOG_AggregationStatus:PLAIN", AGG_STATUS_TABLE,
      System.currentTimeMillis() - 120000, System.currentTimeMillis() + 60000, "ACTIVE");

    AggregationStatusCache.init(dbUrl, SA, "", driver);
    AggregationStatusCache.setStatus(aggStatus);

    props.put("aggregation", aggStatus.AGGREGATION);

    props.put("aggDate", ""+aggDate.getTime());
    props.store(baos, "");
    set.setSchedulingInfo(baos.toString());
    baos.close();

    set.setLatestExecution(aggDate);
    pq.addSet(set);
    pq.houseKeep();


    final PreparedStatement pStmt = testDb.prepareStatement(
      "select AGGREGATION, STATUS from " + AGG_STATUS_TABLE + " where AGGREGATION=?");
    pStmt.setString(1, aggStatus.AGGREGATION);
    
    final ResultSet rset = pStmt.executeQuery();

    while(rset.next()){
      System.out.println("!AGGREGATION " + rset.getString("AGGREGATION"));
      System.out.println("!STATUS " + rset.getString("STATUS"));

    }

  }
	 
	 @Test
	 public void houseKeep() throws Exception {
		 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);
		 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	 
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet.setEarliestExection(new Date(System.currentTimeMillis() - 10000));
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type2", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet2.setLatestExecution(new Date(System.currentTimeMillis() - 10000));
		 pq.addSet(engineSet2);
		 
		 final EngineThread engineSet3 = new EngineThread("type3", Long.valueOf(10L), Logger.getLogger("Test"), new EngineCom());
		 //Set without earliest & latest
		 pq.addSet(engineSet3);
		 
		 final EngineThread engineSet4 = new EngineThread("type3", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 engineSet4.setChangeDate(new Date(System.currentTimeMillis() - 10010 * 60000));
		 //Set without earliest & latest
		 pq.addSet(engineSet4);

		 final EngineThread engineSet5 = new EngineThread("type3", Long.valueOf(15L), Logger.getLogger("Test"), new EngineCom());
		 //Set without earliest & latest
		 pq.addSet(engineSet5);
		 
		 pq.houseKeep();
		 
		 if(engineSet.getEarliestExecution() != null) {
			 fail("EarliestExecution should be null after time has passed");
		 }
		 
		 if(pq.find(engineSet2.getQueueID()) != null) {
			 fail("When latest execution is passed set should have been dropped");
		 }
		 
		 final Iterator<EngineThread> ite = pq.getAll();
		 
		 final EngineThread first = ite.next();
		 // Expecting the one with prio 15
		 if(first != engineSet5) {
			 fail("Sort failed");
		 }
		 
		 final EngineThread second = ite.next();
		 // Expecting the one with prio 10
		 if(second != engineSet3) {
			 fail("Sort failed");
		 }
		 
		 final EngineThread third = ite.next();
		 // Expecting the one with prio 5 promoted to 6
		 if(third != engineSet4) {
			 fail("Sort failed");
		 }
		 
		 if(third.getSetPriority().longValue() != 6L) {
			 fail("Promote priority failed");
		 }
		 
	 }
	 
	 @Test
	 public void synchTest() throws Exception {
	 
		 final Properties props = new Properties();
		 StaticProperties.giveProperties(props);	
			 
		 final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
		 final EngineThread engineSet = new EngineThread("type1", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet);
		 
		 final EngineThread engineSet2 = new EngineThread("type2", Long.valueOf(5L), Logger.getLogger("Test"), new EngineCom());
		 pq.addSet(engineSet2);
		 
		 final Class classpq = pq.getClass();
	
		 final List<Bomber> pool = new ArrayList<Bomber>();
		 		 
		 final Method m_addSet = classpq.getDeclaredMethod("addSet", new Class[] { EngineThread.class } );
		 final Bomber addSet = new AddSet(pq, m_addSet, 3000, "addSet");
		 addSet.start();
		 pool.add(addSet);
		 
		 final Method m_houseKeep = classpq.getDeclaredMethod("houseKeep", new Class[0]);
		 final Bomber houseKeep = new NoParams(pq, m_houseKeep, 3000, "houseKeep");
		 houseKeep.start();
		 pool.add(houseKeep);
		 
		 final Method m_getAll = classpq.getDeclaredMethod("getAll", new Class[0]);
		 final Bomber getAll = new NoParams(pq, m_getAll, 3000, "getAll");
		 getAll.start();
		 pool.add(getAll);
		 
		 final Method m_getAvailable = classpq.getDeclaredMethod("getAvailable", new Class[0]);
		 final Bomber getAvailable = new NoParams(pq, m_getAvailable, 3000, "getAvailable");
		 getAvailable.start();
		 pool.add(getAvailable);
		 
		 // Wait Threads to finish
		 while(true) {
			 boolean lives = false;
			 for(Bomber t : pool) {
				 if(t.isAlive()) {
					 lives = true;
					 Thread.sleep(100);
					 break;
				 }
			 }
			 
			 if(!lives) {
				 break;
			 }
			 
		 }
		 
		 for(Bomber t : pool) {
			 if(t.getException() != null) {
				 fail("Exception caused " + t.getException());
			 } else {
				 Logger.getLogger("PQTest").info(t.getName() + ": " + t.getLoops() + " successfull loops");
			 }
		 }
		 
		 
	 }
	 
	 public class AddSet extends SynchBomber {
		 
		 public AddSet(final PriorityQueue pq, final Method method, final int time, final String name) {
			 super(pq, method, time, name);
		 }
		 
		 @Override
    public Object[] getParams() {
			 return new Object[] { new EngineThread("T" + System.currentTimeMillis(), Long.valueOf(10L), Logger.getLogger("Test"), new EngineCom()) };
		 }
		 
	 }
	 
	 public class NoParams extends SynchBomber {
		 
		 public NoParams(final PriorityQueue pq, final Method method, final int time, final String name) {
			 super(pq, method, time, name);
		 }
		 
		 @Override
    public Object[] getParams() {
			 return new Object[0];
		 }
		 
	 }
	 
	 public interface Bomber {
		 public Exception getException();
		 public boolean isAlive();
		 public int getLoops();
		 public void start();
		 public String getName();
	 }
	 
	 public abstract class SynchBomber extends Thread implements Bomber {
		 
		 private final PriorityQueue pq;
		 private final Method method;
		 private final int time;
		 private Exception exp = null;
		 private int loops;
		 
		 public SynchBomber(final PriorityQueue pq, final Method method, final int time, final String name) {
			 super(name);
			 this.pq = pq;
			 this.method = method;
			 this.time = time;
		 }
		 
		 @Override
		 public void run() {
			 
			 long until = System.currentTimeMillis() + time;
			
			 while(true) {
				 loops++;
				 
				 try {
				
					 method.invoke(pq, getParams());		 
				
				 } catch(Exception e) {
					 this.exp = e;
				 }
					 
				 if(loops % 100 == 0) {
					 if(System.currentTimeMillis() > until) {
						 return;
					 }
				 }
				 
			 }
			 
		 }
		 
		 public abstract Object[] getParams();
		 
		 @Override
    public Exception getException() {
			 return this.exp;
		 }
		 
		 @Override
    public int getLoops() {
			 return loops;
		 }
		 
	 }
	 
	 
	  /**
	   * Testing priority queue resetting. ResetPriorityQueue() method takes
	   * pollInterval and maxPriorityLevel as parameters and changes them
	   * accordingly.
	   */
	  @Test
	  public void testResetPriorityQueue() throws Exception {

	  	final Properties props = new Properties();
	  	StaticProperties.giveProperties(props);	
			 
	  	final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	  	
	    // Asserting that pollInterval and maxPriorityLevel variables are reset to
	    // given values when resetPriorityQueue() method is called
	    pq.resetPriorityQueue(4L, 2);
	    
	    final Class classpq = pq.getClass();
	    final Field pollInterval = classpq.getDeclaredField("pollIntervall");
	    pollInterval.setAccessible(true);
	    final Field maxPriorityLevel = classpq.getDeclaredField("maxPriorityLevel");
	    maxPriorityLevel.setAccessible(true);
	    
	    String actual = "pollInterval: " + pollInterval.getLong(pq) + ", maxPriorityLevel: "
	        + maxPriorityLevel.getInt(pq);
	    String expected = "pollInterval: 4, maxPriorityLevel: 2";
	    assertEquals(expected, actual);
	  }
	 
	  /**
	   * Testing if ID value is added to IDPool vector.
	   */
	  @Test
	  public void testPushID() throws Exception {

	  	final Properties props = new Properties();
	  	StaticProperties.giveProperties(props);	
			 
	  	final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	  	
	    // Reflecting the tested method
	    Class classpq = pq.getClass();
	    
	    final Method pushID = classpq.getDeclaredMethod("pushID", new Class[] { Long.class });
	    pushID.setAccessible(true);
	    
	    final Field IDPool = classpq.getDeclaredField("idPool");
	    IDPool.setAccessible(true);

	    // Making sure IDPool vector is empty
	    List<Long> actualPool = (List<Long>)IDPool.get(pq);
	    if (actualPool.isEmpty()) {
	      // Checking if ID is added to the vector when pushID() method is invoked
	      pushID.invoke(pq, new Object[] { 8L });
	      assertEquals(1, actualPool.size());
	    } else {
	      fail("Test Failed - IDPool was not empty");
	    }
	  }

	  /**
	   * Testing that doublicate ID values are not added to IDPool vector.
	   */
	  @Test
	  public void testPushIDTwiceWithSameValue() throws Exception {

	  	final Properties props = new Properties();
	  	StaticProperties.giveProperties(props);	
			 
	  	final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	  	
	    // Reflecting the tested method
	    Class classpq = pq.getClass();
	    
	    Method pushID = classpq.getDeclaredMethod("pushID", new Class[] { Long.class });
	    pushID.setAccessible(true);

	    // Adding ID value to IDPool
	    pushID.invoke(pq, new Object[] { 101L });

	    final Field IDPool = classpq.getDeclaredField("idPool");
	    IDPool.setAccessible(true);
	    
	    // Making sure IDPool vector has one item
	    List<Long> actualPool = (List<Long>) IDPool.get(pq);
	    if (actualPool.size() == 1) {
	      // Checking that the same ID is not added to the vector twice when
	      // pushID() method is invoked
	      pushID.invoke(pq, new Object[] { 101L });
	      assertEquals(1, actualPool.size());
	    } else {
	      fail("Test Failed - IDPool was empty or had more than one element");
	    }
	  }

	  /**
	   * Testing if first element is moved from IDPool vector when popID() method is
	   * called.
	   */
	  @Test
	  public void testPopID() throws Exception {

	  	final Properties props = new Properties();
	  	StaticProperties.giveProperties(props);	
			 
	  	final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	  	
	    // Reflecting the tested method
	    Class classpq = pq.getClass();
	    
	    final Method popID = classpq.getDeclaredMethod("popID", new Class[] {});
	    final Method pushID = classpq.getDeclaredMethod("pushID", new Class[] { Long.class });
	    popID.setAccessible(true);
	    pushID.setAccessible(true);

	    // Adding ID value to IDPool which will later be removed
	    pushID.invoke(pq, new Object[] { 88L });

	    final Field IDPool = classpq.getDeclaredField("idPool");
	    IDPool.setAccessible(true);
	    
	    // Making sure IDPool vector has one item
	    List<Long> actualPool = (List<Long>) IDPool.get(pq);
	    if (actualPool.size() == 1) {
	      // Checking that the ID is removed from IDPool
	      assertEquals(88L, popID.invoke(pq, new Object[] {}));
	      assertEquals(0, actualPool.size());
	    } else {
	      fail("Test Failed - IDPool was empty or had more than one element");
	    }
	  }

	  /**
	   * Testing element removing from empty IDPool. Nothing is removed but new ID
	   * value is created.
	   */
	  @Test
	  public void testPopIDWithEmptyIDPool() throws Exception {

	  	final Properties props = new Properties();
	  	StaticProperties.giveProperties(props);	
			 
	  	final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	  	
	    // Reflecting the tested method
	    Class classpq = pq.getClass();
	    
	    final Method popID = classpq.getDeclaredMethod("popID", new Class[] {});
	    popID.setAccessible(true);

	    final Field IDPool = classpq.getDeclaredField("idPool");
	    IDPool.setAccessible(true);
	    
	    // Making sure IDPool vector is empty
	    List<Long> actualPool = (List<Long>) IDPool.get(pq);
	    if (actualPool.isEmpty()) {
	      // Checking that new ID value is created
	      assertEquals(0L, popID.invoke(pq, new Object[] {}));
	      assertEquals(0, actualPool.size());
	    } else {
	      fail("Test Failed - IDPool was not empty");
	    }
	  }
	  
	  @Test
	  public void settersNGetters() throws Exception {

	  	final Properties props = new Properties();
	  	StaticProperties.giveProperties(props);	
			 
	  	final PriorityQueue pq = new PriorityQueue(1000, 15, new MyPersHandler(), esph);
	  	pq.setActive(false);
	  	
	  	if(pq.isActive()) {
	  		fail("setActive did not work");
	  	}
	  	
	  	pq.setPollIntervall(10L);
	  	
	  	if(pq.getPollIntervall() != 10L) {
	  		fail("setPollIntervall did not work");
	  	}
	  
	  	if(pq.getPersistenceHandler() == null) {
	  		fail("PersistenceHandler is not getting set");
	  	}
	  	
	  }
	  	
	  public class MyPersHandler extends PersistenceHandler {

	  	public MyPersHandler() {
	  	}
	  		
	  	@Override
      public void newSet(final EngineThread et) {
	  		et.setPersistentID(System.nanoTime());
	  	}

	  	@Override
      public void droppedSet(final EngineThread et) {
	  	}
	  	
	  	@Override
      public void executedSet(final EngineThread et) {
	  	}

	  	public List<EngineThread> getSets() throws Exception {
	  		
	  		final List<EngineThread> sets = new ArrayList<EngineThread>();
	  		return sets;
	  		
	  	}
	  
	  };
	 
}