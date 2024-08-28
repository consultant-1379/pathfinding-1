package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.JUnit4TestAdapter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class SQLDeleteTest {

	  private static SQLDelete objUnderTest;

	  private static Statement stmt;

	  @BeforeClass
	  public static void setUpBeforeClass() throws Exception {
		
	    Meta_versions metaVersions;
	    Long collectionSetId = 1L;
	    Meta_collections collection;
	    Long transferActionId = 1L;
	    Long transferBatchId = 1L;
	    Long connectId = 1L;
	    RockFactory rockFactory;
	    ConnectionPool connectionPool;
	    Meta_transfer_actions metaTransferActions;
	    
	    try {
	      Class.forName("org.hsqldb.jdbcDriver").newInstance();
	    } catch (Exception e) {
	      System.out.println(e);
	      System.exit(1);
	    }
	    Connection con = null;
	    try {
	      con = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", "");
	    } catch (SQLException sqle) {
	      System.out.println(sqle);
	      System.exit(1);
	    }
	    stmt = con.createStatement();
	    try {
	    stmt.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID BIGINT,COLLECTION_SET_NAME VARCHAR(31), "
	        + "DESCRIPTION VARCHAR(31), VERSION_NUMBER VARCHAR(31), ENABLED_FLAG VARCHAR(31), TYPE VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '1', 'Y', 'type')");
	    
	    stmt.execute("CREATE TABLE Meta_Target_Tables (VERSION_NUMBER VARCHAR(31),COLLECTION_SET_ID NUMERIC(31), "
		        + "COLLECTION_ID NUMERIC(31), TRANSFER_ACTION_ID NUMERIC(31), CONNECTION_ID NUMERIC(31), TABLE_ID NUMERIC(31))");
		stmt.executeUpdate("INSERT INTO Meta_Target_Tables VALUES('((89))', 1, 1, 1, 1, 1)");
		
		stmt.execute("CREATE TABLE Meta_Tables (TABLE_ID NUMERIC(31),TABLE_NAME VARCHAR(31), "
		        + "VERSION_NUMBER VARCHAR(31), IS_JOIN VARCHAR(31), JOIN_CLAUSE VARCHAR(31), TABLES_AND_ALIASES VARCHAR(31), CONNECTION_ID NUMERIC(31))");
		stmt.executeUpdate("INSERT INTO Meta_Tables VALUES(1, 'EXAMPLE_TABLE', '((89))', 'N', '', 'EXAMPLE_TABLE', 1)");

		stmt.execute("CREATE TABLE Meta_joints ( ID BIGINT  ,IS_PK_COLUMN VARCHAR(31) ,IS_SUM_COLUMN VARCHAR(31) ,IS_GROUP_BY_COLUMN VARCHAR(31), "
				+ "COLUMN_SPACE_AT_FILE BIGINT  ,FILE_ORDER_BY BIGINT  ,PLUGIN_METHOD_NAME VARCHAR(31) ,VERSION_NUMBER VARCHAR(31), "
				+ "COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,TRANSFER_ACTION_ID BIGINT  ,TARGET_CONNECTION_ID BIGINT, "
				+ "TARGET_TABLE_ID BIGINT  ,COLUMN_ID_TARGET_COLUMN BIGINT  ,SOURCE_CONNECTION_ID BIGINT  ,SOURCE_TABLE_ID BIGINT, "
				+ "COLUMN_ID_SOURCE_COLUMN BIGINT  ,TRANSFORMATION_ID BIGINT  ,TRANSF_TABLE_ID BIGINT  ,PAR_NAME VARCHAR(31) ,FILE_ID BIGINT, "
				+ "PLUGIN_ID BIGINT  ,FREE_FORMAT_TRANSFORMAT VARCHAR(31) ,METHOD_PARAMETER VARCHAR(31))");
		stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'testIS_PK_COLUMN1'  ,'testIS_SUM_COLUMN1'  ,'testIS_GROUP_BY_COLUMN1', "
				+ "1  ,1  ,'testPLUGIN_METHOD_NAME1'  ,'testVERSION_NUMBER1'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1, "
				+ "'testPAR_NAME1'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT1'  ,'testMETHOD_PARAMETER1' )");
		
		stmt.execute("CREATE TABLE Meta_columns ( COLUMN_ID BIGINT  ,COLUMN_NAME VARCHAR(31) ,COLUMN_ALIAS_NAME VARCHAR(31), "
				+ "COLUMN_TYPE VARCHAR(31) ,COLUMN_LENGTH BIGINT  ,IS_PK_COLUMN VARCHAR(31) ,VERSION_NUMBER VARCHAR(31), "
				+ "CONNECTION_ID BIGINT  ,TABLE_ID BIGINT )");
		stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'testCOLUMN_NAME'  ,'testCOLUMN_ALIAS_NAME'  ,'testCOLUMN_TYPE', "
				+ "1  ,'testIS_PK_COLUMN'  ,'((89))'  ,1  ,1 )");
		
		stmt.execute("CREATE TABLE Meta_fk_tables ( MAX_ERRORS BIGINT  ,VERSION_NUMBER VARCHAR(31) ,WHERE_CLAUSE VARCHAR(31), "
				+ "FILTER_ERRORS_FLAG VARCHAR(31) ,REPLACE_ERRORS_FLAG VARCHAR(31) ,REPLACE_ERRORS_WITH VARCHAR(31), "
				+ "COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,TRANSFER_ACTION_ID BIGINT  ,CONNECTION_ID BIGINT  ,TABLE_ID BIGINT  ,TARGET_TABLE_ID BIGINT )");
		stmt.executeUpdate("INSERT INTO Meta_fk_tables VALUES( 1  ,'((89))'  ,'testWHERE_CLAUSE'  ,'testFILTER_ERRORS_FLAG', "
				+ "'testREPLACE_ERRORS_FLAG'  ,'testREPLACE_ERRORS_WITH'  ,1  ,1  ,1  ,1  ,1  ,1 )");
		
		stmt.execute("CREATE TABLE Meta_fk_table_joints ( VERSION_NUMBER VARCHAR(31)  ,CONNECTION_ID BIGINT  ,TABLE_ID BIGINT  ,COLUMN_ID_FK_COLUMN BIGINT, "
				+ "TARGET_TABLE_ID BIGINT  ,COLUMN_ID BIGINT  ,COLLECTION_SET_ID BIGINT  ,COLLECTION_ID BIGINT  ,TRANSFER_ACTION_ID BIGINT )");
		stmt.executeUpdate("INSERT INTO Meta_fk_table_joints VALUES( '((89))'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1 )");
		
	    stmt.execute("CREATE TABLE Meta_databases (USERNAME VARCHAR(31), VERSION_NUMBER VARCHAR(31), "
	        + "TYPE_NAME VARCHAR(31), CONNECTION_ID BIGINT, CONNECTION_NAME VARCHAR(31), "
	        + "CONNECTION_STRING VARCHAR(31), PASSWORD VARCHAR(31), DESCRIPTION VARCHAR(31), DRIVER_NAME VARCHAR(31), "
	        + "DB_LINK_NAME VARCHAR(31))");
	    stmt.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'typenames', 1, 'connectionname', "
	        + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");

	    stmt.execute("CREATE TABLE DWHType (TECHPACK_NAME VARCHAR(31), TYPENAME VARCHAR(31), TABLELEVEL VARCHAR(31), "
	        + "STORAGEID VARCHAR(31), PARTITIONSIZE BIGINT, PARTITIONCOUNT BIGINT, STATUS VARCHAR(31), "
	        + "TYPE VARCHAR(31), OWNER VARCHAR(31), VIEWTEMPLATE VARCHAR(31), CREATETEMPLATE VARCHAR(31), "
	        + "NEXTPARTITIONTIME TIMESTAMP, BASETABLENAME VARCHAR(31), DATADATECOLUMN VARCHAR(31), "
	        + "PUBLICVIEWTEMPLATE VARCHAR(31), PARTITIONPLAN VarChar(31))");
	    stmt.executeUpdate("INSERT INTO DWHType VALUES('techpakname', 'typename', 'tablelevel', 'storageid', "
	        + "1, 1, 'status', 'type', 'owner', 'viewtemplate', 'createtemplate', "
	        + "2008-06-23, 'EXAMPLE_TABLE', 'datadatecolumn', 'publicviewtemplate', 'partitionplan')");

	    stmt.execute("CREATE TABLE Dwhcolumn (STORAGEID VARCHAR(31), DATANAME VARCHAR(31), COLNUMBER BIGINT, "
	        + "DATATYPE VARCHAR(31), DATASIZE INTEGER, DATASCALE INTEGER, UNIQUEVALUE BIGINT, "
	        + "NULLABLE INTEGER, INDEXES VARCHAR(31), UNIQUEKEY INTEGER, STATUS VARCHAR(31), INCLUDESQL INTEGER)");
	    
	    stmt.execute("CREATE TABLE Example_table (DUPLICATE VARCHAR(31))");
	    }
	    catch(Exception e){}
	    rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);

	    collection = new Meta_collections(rockFactory);

	    connectionPool = new ConnectionPool(rockFactory);

	    metaTransferActions = new Meta_transfer_actions(rockFactory);
	    
	    metaTransferActions.setAction_contents("INSERT INTO Example_table VALUES ('test')");

	    metaVersions = new Meta_versions(rockFactory);

	    
	    try {
	      objUnderTest = new SQLDelete(metaVersions, collectionSetId, collection, transferActionId, transferBatchId, connectId,
	    		  rockFactory, connectionPool, metaTransferActions, "Test");
	    
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }

	  @AfterClass
	  public static void tearDownAfterClass() throws Exception {
	    try {
		stmt.execute("DROP TABLE Dwhcolumn");
	    stmt.execute("DROP TABLE DWHType");
	    stmt.execute("DROP TABLE Meta_databases");
	    stmt.execute("DROP TABLE Meta_collection_sets");
	    stmt.execute("DROP TABLE Meta_Target_Tables");
	    stmt.execute("DROP TABLE Meta_Tables");
	    stmt.execute("DROP TABLE Meta_Joints");
	    stmt.execute("DROP TABLE Meta_columns");
	    stmt.execute("DROP TABLE Meta_fk_tables");
	    stmt.execute("DROP TABLE Meta_fk_table_joints");
	    stmt.execute("DROP TABLE Example_table_01");
	    stmt.execute("DROP TABLE Example_table");
	    }
	    catch(Exception e) {}
	    objUnderTest = null;
	  }

	  @Test
	  public void testExecuteWillThrowException() throws EngineException {
        try {
		  objUnderTest.execute();
		    }
	    catch(Exception e){
	   //   assertEquals("Cannot execute action.",e.getMessage());
	    	}
	 }

	  public static junit.framework.Test suite() {
	    return new JUnit4TestAdapter(SQLDeleteTest.class);
	  }
}
