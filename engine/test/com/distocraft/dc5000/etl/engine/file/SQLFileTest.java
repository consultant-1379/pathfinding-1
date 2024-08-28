package com.distocraft.dc5000.etl.engine.file;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_columns;
import com.distocraft.dc5000.etl.rock.Meta_tables;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

import java.io.File;
import java.sql.SQLException;

import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class SQLFileTest extends UnitDatabaseTestCase {

	static Meta_transfer_actions metaTransferActions;
	static Meta_versions metaVersions;
	static Meta_collections collection;
	static Meta_columns objMetaCol;
	static Meta_tables metaTables;
	static ConnectionPool  connectionPool;
	
    Long collectionSetId = 1L;
    Long transferActionId = 1L;
    Long transferBatchId = 1L;
    Long connectId = 1L;
    
    static RockFactory etlrep ;
    static RockFactory dwhrep ;
    
    static String batchColumnName;
	
  @BeforeClass
  public static void setUp() throws Exception{
	  
	setup(TestType.unit);
	
    loadDefaultTechpack(TechPack.stats, "v1");
    
    etlrep = getRockFactory(Schema.etlrep);
   
    Statement stmt = etlrep.getConnection().createStatement();
    
    try {
    	
    	stmt.executeUpdate("INSERT INTO Meta_files VALUES( 1  ,'testF_NAME'  ,'testF_C_T'  ,'\n', "
    			+ "'\t'  ,1  ,1  ,1  ,'Y'  ,'((1))'  ,1 )");
    	
    	stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '((1))', 'Y', 'type')");
        
        stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ,'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");
               
    }
    catch(SQLException sqlE) {
    	System.out.println("SQLException :" + sqlE);
    }
    
    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaVersions = new Meta_versions(etlrep);
    metaTables = new Meta_tables(etlrep);
    collection = new Meta_collections(etlrep);
    connectionPool = new ConnectionPool(etlrep);
  }
  
  @Test
  public void testgetFile() throws Exception {
	  try {	  
		  SQLFile sf = new SQLFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, metaTransferActions, batchColumnName);
		  File f = sf.getFile();
		  File f1 = new File("testF_NAME");
		  assertEquals(f1, f);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getFile: " + e);
	  }
  }
  
  @Test
  public void testgetRowDelim() throws Exception {
	  try {	  
		  SQLFile sf = new SQLFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, metaTransferActions, batchColumnName);
		  String actual = sf.getRowDelim();
		  assertEquals("\n", actual);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getRowDelim: " + e);
	  }
  }
  
  @Test
  public void testgetColumnDelim() throws Exception {
	  try {	  
		  SQLFile sf = new SQLFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, metaTransferActions, batchColumnName);
		  String actual = sf.getColumnDelim();
		  assertEquals("\t", actual);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getColumnDelim: " + e);
	  }
  }
  
  @Test
  public void testgetFillWithBlanks() throws Exception {
	  try {	  
		  SQLFile sf = new SQLFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, metaTransferActions, batchColumnName);
		  Boolean actual = sf.getFillWithBlanks();
		  Boolean expected = false;
		  assertEquals(expected, actual);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getFillWithBlanks: " + e);
	  }
  }
  
  @Test
  public void testgetCommitAfterNRows() throws Exception {
	  try {	  
		  SQLFile sf = new SQLFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, metaTransferActions, batchColumnName);
		  int actual = sf.getCommitAfterNRows();
		  assertEquals(1, actual);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getCommitAfterNRows: " + e);
	  }
  }
  
  @Test
  public void testgetBatchColumnName() throws Exception {
	  try {	  
		  SQLFile sf = new SQLFile(metaVersions, 1L, collection, 1L, 1L, 1L, etlrep, metaTransferActions, batchColumnName);
		  String actual = sf.getBatchColumnName();
		  assertEquals(null, actual);
	  }
	  catch(Exception e) {
		 System.out.println("Exception in getBatchColumnName: " + e);
	  }
  }
  
}
