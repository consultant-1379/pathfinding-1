package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache.PTableEntry;

public class HistorySQLExecuteTest {

  private static RockFactory rockFact;

  static {
    final Logger root = Logger.getLogger("");
    root.setLevel(Level.FINEST);
    for (Handler handler : root.getHandlers()) {
      handler.setLevel(Level.FINEST);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  	final Statement stmt = rockFact.getConnection().createStatement();
  	stmt.execute("DROP TABLE Meta_Collection_Sets");
  	stmt.execute("DROP TABLE META_DATABASES");
  	stmt.execute("DROP TABLE DWHPartition");
  	stmt.execute("DROP TABLE TEST_RESULT");
  	stmt.execute("DROP TABLE table_RAW_01");
  	stmt.execute("DROP TABLE table_RAW_02");
  	stmt.execute("DROP TABLE table_RAW_03");
  	
  	stmt.close();
  	rockFact.getConnection().close();
  }
  
  @BeforeClass
  public static void init() throws Exception {

    StaticProperties.giveProperties(new Properties());
    try {
      Class.forName("org.hsqldb.jdbcDriver");
      rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
      final Statement stmt = rockFact.getConnection().createStatement();

      stmt.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(31), COLLECTION_SET_NAME VARCHAR(128), DESCRIPTION VARCHAR(255), VERSION_NUMBER VARCHAR(31), ENABLED_FLAG VARCHAR(31), TYPE VARCHAR(31))");
      stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES ('1', 'set_name', 'description', '2', 'Y', 'type')");

      stmt.execute("CREATE TABLE Meta_databases (USERNAME varchar(30), VERSION_NUMBER varchar(32), TYPE_NAME varchar(15), CONNECTION_ID numeric(31), CONNECTION_NAME varchar(30), CONNECTION_STRING varchar(200), PASSWORD varchar(30), DESCRIPTION varchar(32000), DRIVER_NAME varchar(100), DB_LINK_NAME varchar(128))");
      stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 0, 'etlrep', 'jdbc:hsqldb:mem:testdb', '', '', 'org.hsqldb.jdbcDriver', null)");
      stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 1, 'dwhrep', 'jdbc:hsqldb:mem:testdb', '', '', 'org.hsqldb.jdbcDriver', null)");
      stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 2, 'dwh', 'jdbc:hsqldb:mem:testdb', '', '', 'org.hsqldb.jdbcDriver', null)");
      
      stmt.execute("CREATE TABLE DWHPartition(STORAGEID varchar(12), TABLENAME varchar(12), STARTTIME timestamp, ENDTIME timestamp, STATUS varchar(12), LOADORDER int)");
      stmt.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_01', '1970-01-01 01:00:00.0', '1970-01-01 01:00:00.0', 'ACTIVE', 3)");
      stmt.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_02', '1970-01-01 01:00:00.0', '1970-01-01 01:00:00.0', 'ACTIVE', 2)");
      stmt.executeUpdate("insert into DWHPartition VALUES('table:RAW', 'table_RAW_03', '1970-01-01 01:00:00.0', '1970-01-01 01:00:00.0', 'ACTIVE', 1)");
      
      stmt.execute("create table table_RAW_01 (tableName varchar(255))");
      stmt.execute("create table table_RAW_02 (tableName varchar(255))");
      stmt.execute("create table table_RAW_03 (tableName varchar(255))");
      
      stmt.execute("create table TEST_RESULT (tableName varchar(255))");
		
      stmt.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed: " + e.getMessage());
    }
  }
  
  private HistorySQLExecute createTestInstance() {
	  final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);
      final Properties whereProps = new Properties();
      whereProps.setProperty("typename", "DC_E_LTE_SONV_CM_CELL_GIS");
      try {
		act.setWhere_clause(TransferActionBase.propertiesToString(whereProps));
		act.setAction_contents("INSERT INTO TEST_RESULT SELECT 'DC_E_LTE_SONV_CM_CELL_GIS_HIST_RAW' from Meta_collection_sets");
		final HistorySQLExecute testInstance = new HistorySQLExecute(new Meta_versions(rockFact), Long.valueOf(1),
	            new Meta_collections(rockFact), Long.valueOf(1), Long.valueOf(1), Long.valueOf(1), rockFact,
	            new ConnectionPool(rockFact), act, Logger.getLogger("HistorySQLExecuteTest"));
		return testInstance;
      } catch (Exception e) {
  		e.printStackTrace();
  		return null;
      }
  } //createTestInstance
  
  private void insertRows(final String table, long numRows) {
	  Statement stmt;
	  long startMs = System.currentTimeMillis();
	  try {
		stmt = rockFact.getConnection().createStatement();
		for (long j = 0; j < numRows; j++) {
			stmt.execute("INSERT INTO "+table+" VALUES('"+j+"');");
		}	
		stmt.close();
	  } catch (SQLException e) {
	  }
	  long endMs = System.currentTimeMillis();
	  //Get count of rows
	  HistorySQLExecute action = createTestInstance();
	  Long rowCount = (long) 0;
	  try {
		rowCount = action.getTableRowCount(table);
	  } catch (Exception e) {}
//	  System.out.println("Inserted "+numRows+" rows into table:"+table+" in "+(endMs-startMs)+"ms, now has "+rowCount+" rows.");//TODO:Comment out/Remove.
  }//insertRows
  
  /**
   * Test HistorySQLExecute.execute
   * Expected output is a single partition for the History.
   * typename="DC_E_LTE_SONV_CM_CELL_GIS" => "DC_E_LTE_SONV_CM_CELL_GIS_HIST_RAW"
   * @throws Exception
   */
  @Test
  public void testExecute() throws Exception {
	HistorySQLExecute action = createTestInstance();
    // Test
    action.execute();

    //Check result
    final Connection c = rockFact.getConnection();
    Statement st = c.createStatement();
    final ResultSet rs = st.executeQuery("Select tableName from TEST_RESULT");
    if (rs.next()) {
      assertEquals("DC_E_LTE_SONV_CM_CELL_GIS_HIST_RAW", rs.getString(1));
    } else {
      fail("Only 1 row was expected, got >1.");
    }

    rs.close();
    st.close();
  } //testExecute
  
  @Test
  public void testTruncateTable() throws Exception {
	final Statement stmt = rockFact.getConnection().createStatement();
	  stmt.executeUpdate("CREATE TABLE ATABLE (col1 varchar(255), col2 varchar(255), col3 varchar(255))");
	  stmt.executeUpdate("INSERT INTO ATABLE VALUES('1','','');");
	  stmt.executeUpdate("INSERT INTO ATABLE VALUES('2','','');");
	  stmt.executeUpdate("INSERT INTO ATABLE VALUES('3','','');");
	  stmt.close();
  	HistorySQLExecute action = createTestInstance();
    // Test
	try {
		action.truncateTable("ATABLE");
	} catch (SQLException e) {
		fail("Exception for TRUNCATE statement:"+e);
	}
	//Check result
    final Connection c = rockFact.getConnection();
    Statement st = c.createStatement();
    final ResultSet rs = st.executeQuery("Select count(*) from ATABLE");
    if (rs.next()) {
      assertEquals("0", rs.getString(1));
    } else {
      fail("Table should be emptied, zero rows were expected, got >0");
    }
    rs.close();
    st.close();
  }//testTruncateTable
  
  @Test
  public void testGetTableRowCount() throws Exception {
	  final Statement stmt = rockFact.getConnection().createStatement();
		stmt.executeUpdate("CREATE TABLE COUNTTABLE (col1 varchar(255), col2 varchar(255), col3 varchar(255))");
		stmt.executeUpdate("INSERT INTO COUNTTABLE VALUES('1','','');");
		stmt.executeUpdate("INSERT INTO COUNTTABLE VALUES('2','','');");
		stmt.executeUpdate("INSERT INTO COUNTTABLE VALUES('3','','');");
		stmt.close();
		
	  HistorySQLExecute action = createTestInstance();
	  // Test
	  if (action.getTableRowCount("COUNTTABLE") != 3) {
			fail("getRowCount does not work, 3 rows expected, got:"+action.getTableRowCount("COUNTTABLE"));
	  }
  }//testGetTableRowCount
  
  @Test
  public void testGetListOfPartitions() throws Exception {
	  PhysicalTableCache.testInit(null);
		PhysicalTableCache ptc = PhysicalTableCache.getCache();
		
		final List<PhysicalTableCache.PTableEntry> list = new ArrayList<PhysicalTableCache.PTableEntry> ();
		final PhysicalTableCache.PTableEntry entry3 = ptc.new PTableEntry();
		entry3.storageID = "table:RAW";
		entry3.tableName = "table_RAW_03";
		entry3.startTime = 0L;
		entry3.endTime = System.currentTimeMillis();
		entry3.status = "ACTIVE";
		entry3.partitionsize = 1000;
		entry3.defaultpartitionsize = 1000;
		entry3.loadOrder = 3;
		list.add(entry3);
		
		final PhysicalTableCache.PTableEntry entry2 = ptc.new PTableEntry();
		entry2.storageID = "table:RAW";
		entry2.tableName = "table_RAW_02";
		entry2.startTime = 0L;
		entry2.endTime = System.currentTimeMillis();
		entry2.status = "ACTIVE";
		entry2.partitionsize = 1000;
		entry2.defaultpartitionsize = 1000;
		entry2.loadOrder = 2;
		list.add(entry2);
		
		final PhysicalTableCache.PTableEntry entry1 = ptc.new PTableEntry();
		entry1.storageID = "table:RAW";
		entry1.tableName = "table_RAW_01";
		entry1.startTime = 0L;
		entry1.endTime = System.currentTimeMillis();
		entry1.status = "ACTIVE";
		entry1.partitionsize = 1000;
		entry1.defaultpartitionsize = 1000;
		entry1.loadOrder = 1;
		list.add(entry1);
		
		
		
		final Map<String, List<PhysicalTableCache.PTableEntry>> ptmap = new HashMap<String, List<PhysicalTableCache.PTableEntry>> ();
		ptmap.put(entry3.storageID, list);

		PhysicalTableCache.testInit(ptmap);
	  HistorySQLExecute action = createTestInstance();
	  // Test
	  List<PTableEntry> result = action.getListOfPartitions("table:RAW");
      assertEquals("table_RAW_03", result.get(0).tableName);//Expect _03 first.
      assertEquals("table_RAW_02", result.get(1).tableName);
      assertEquals("table_RAW_01", result.get(2).tableName);
  }//testGetListOfPartitions
  
  @Test
  public void testGetPartition() throws Exception {
	  	PhysicalTableCache.initialize(rockFact);
		PhysicalTableCache ptc = PhysicalTableCache.getCache();
		
		final List<PhysicalTableCache.PTableEntry> list = new ArrayList<PhysicalTableCache.PTableEntry> ();
		final PhysicalTableCache.PTableEntry entry1 = ptc.new PTableEntry();
		entry1.storageID = "table:RAW";
		entry1.tableName = "table_RAW_01";
		entry1.startTime = 0L;
		entry1.endTime = System.currentTimeMillis();
		entry1.status = "ACTIVE";
		entry1.partitionsize = 1000;
		entry1.defaultpartitionsize = 1000;
		entry1.loadOrder = 0;
		list.add(entry1);
		
		final PhysicalTableCache.PTableEntry entry2 = ptc.new PTableEntry();
		entry2.storageID = "table:RAW";
		entry2.tableName = "table_RAW_02";
		entry2.startTime = 0L;
		entry2.endTime = System.currentTimeMillis();
		entry2.status = "ACTIVE";
		entry2.partitionsize = 2000;
		entry2.defaultpartitionsize = 1000;
		entry2.loadOrder = 0;
		list.add(entry2);
		
		final PhysicalTableCache.PTableEntry entry3 = ptc.new PTableEntry();
		entry3.storageID = "table:RAW";
		entry3.tableName = "table_RAW_03";
		entry3.startTime = 0L;
		entry3.endTime = System.currentTimeMillis();
		entry3.status = "ACTIVE";
		entry3.partitionsize = 300;
		entry3.defaultpartitionsize = 1000;
		entry3.loadOrder = 0;
		list.add(entry3);
		insertRows("table_RAW_03", 301);// Want _03 to be full
		
		final Map<String, List<PhysicalTableCache.PTableEntry>> ptmap = new HashMap<String, List<PhysicalTableCache.PTableEntry>> ();
		ptmap.put(entry3.storageID, list);

		PhysicalTableCache.testInit(ptmap);
	  HistorySQLExecute action = createTestInstance();
	  // Test 
	  String result = action.getPartition("table:RAW");
      assertEquals("table_RAW_02", result); //First was setup full, should get _02
      // Test (insert 1 row into _02, should still get _02)
      insertRows("table_RAW_02", 1);
      result = action.getPartition("table:RAW");
      assertEquals("table_RAW_02", result);//should still get _02
      // Add another row, should be same partition and have 2 rows.
      insertRows("table_RAW_02", 1);
      result = action.getPartition("table:RAW");
      assertEquals("table_RAW_02", result);//should still get _02
      if (!(action.getTableRowCount("table_RAW_02")==2)) {
    	  fail("Not full partition was incorrectly truncated, expected 2/2000 rows, got:"+action.getTableRowCount("table_RAW_02"));
      }
      // Test (fill _02)
      insertRows("table_RAW_02", 2000);
      result = action.getPartition("table:RAW");
      assertEquals("table_RAW_01", result); // _02 was filled, should get _01 (lowest loadOrder)
      // Test (have all partitions full)
      insertRows("table_RAW_01", 1001);
      result = action.getPartition("table:RAW");
      assertEquals("table_RAW_03", result);
      // Test (partition should be truncated)
	  result = action.getPartition("table:RAW");
      assertEquals("table_RAW_03", result); //was full, should now be emptied
      if (!(action.getTableRowCount("table_RAW_03")==0)) {
    	  fail("Partition to be used was not truncated, rows:"+action.getTableRowCount("table_RAW_01"));
      }
  }//testGetPartition
  
  /**
   * Test when all partitions are full, that subsequent inserts go in a round-robin fashion.
   * Tests loadOrder gets set correctly and truncating a full partition.
   */
  @Test
  public void testFullPartitions() throws Exception {
	    int MAX_ROWS = 100;
  		PhysicalTableCache.initialize(rockFact);
		PhysicalTableCache ptc = PhysicalTableCache.getCache();
		
		final List<PhysicalTableCache.PTableEntry> list = new ArrayList<PhysicalTableCache.PTableEntry> ();
		final PhysicalTableCache.PTableEntry entry1 = ptc.new PTableEntry();
		entry1.storageID = "table:RAW";
		entry1.tableName = "table_RAW_01";
		entry1.startTime = 0L;
		entry1.endTime = System.currentTimeMillis();
		entry1.status = "ACTIVE";
		entry1.partitionsize = MAX_ROWS;
		entry1.defaultpartitionsize = 1000;
		entry1.loadOrder = 0;
		list.add(entry1);
		insertRows("table_RAW_01", MAX_ROWS);// full
		
		final PhysicalTableCache.PTableEntry entry2 = ptc.new PTableEntry();
		entry2.storageID = "table:RAW";
		entry2.tableName = "table_RAW_02";
		entry2.startTime = 0L;
		entry2.endTime = System.currentTimeMillis();
		entry2.status = "ACTIVE";
		entry2.partitionsize = MAX_ROWS;
		entry2.defaultpartitionsize = 1000;
		entry2.loadOrder = 0;
		list.add(entry2);
		insertRows("table_RAW_02", MAX_ROWS);// full
		
		final PhysicalTableCache.PTableEntry entry3 = ptc.new PTableEntry();
		entry3.storageID = "table:RAW";
		entry3.tableName = "table_RAW_03";
		entry3.startTime = 0L;
		entry3.endTime = System.currentTimeMillis();
		entry3.status = "ACTIVE";
		entry3.partitionsize = MAX_ROWS;
		entry3.defaultpartitionsize = 1000;
		entry3.loadOrder = 0;
		list.add(entry3);
		insertRows("table_RAW_03", MAX_ROWS);// full
		
		final Map<String, List<PhysicalTableCache.PTableEntry>> ptmap = new HashMap<String, List<PhysicalTableCache.PTableEntry>> ();
		ptmap.put(entry3.storageID, list);

		PhysicalTableCache.testInit(ptmap);
		HistorySQLExecute action = createTestInstance();
		// Test (Round-robin mechanism)
		String result = action.getPartition("table:RAW");
		assertEquals("table_RAW_01", result);
		//Need to refill after it was truncated
		insertRows("table_RAW_01", MAX_ROWS);// full
		// Should move onto next partition, by loadOrder.
		result = action.getPartition("table:RAW");
		assertEquals("table_RAW_03", result);
		//Need to refill after it was truncated
		insertRows("table_RAW_03", MAX_ROWS);// full
		// Should move onto next partition, by loadOrder.
		result = action.getPartition("table:RAW");
		assertEquals("table_RAW_02", result);
		//Need to refill after it was truncated
		insertRows("table_RAW_02", MAX_ROWS);// full
		// Back to first
		result = action.getPartition("table:RAW");
		assertEquals("table_RAW_01", result);
  }//testFullPartitions

}
