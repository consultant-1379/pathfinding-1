package com.distocraft.dc5000.etl.engine.sql;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DuplicateCheckActionTest extends UnitDatabaseTestCase {

  private DuplicateCheckAction objUnderTest = null;
  private static Meta_transfer_actions metaTransferActions;
  private static Meta_versions metaVersions;
  private static Meta_collections collection;
  private static ConnectionPool connectionPool;
  private static Statement saStmt;
  private final static SetContext sctx = new SetContext();

  static TransferActionBase transferActionBase;

  Long collectionSetId = 1L;
  Long transferActionId = 1L;
  Long transferBatchId = 1L;
  static Long connectId = 1L;

  static RockFactory etlrep;
  static RockFactory dwhrep;

  static String batchColumnName;


  //static Connection con = null;

  @BeforeClass
  public static void setUp() throws Exception {

    final Map<String, Long> connecionIdMap = setup(TestType.unit);
    connectId = connecionIdMap.get("dwh:USER");

    loadDefaultTechpack(TechPack.stats, "v1");
    loadDefaultTechpack(TechPack.stats, "v2");

    etlrep = getRockFactory(Schema.etlrep);
    dwhrep = getRockFactory(Schema.dwhrep);
    Statement stmt = etlrep.getConnection().createStatement();

    try {
      stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'set_name', 'description', '1', 'Y', 'type')");

      stmt.executeUpdate("INSERT INTO Meta_Target_Tables VALUES('((89))', 1, 1, 1, 1, 1)");

      stmt.executeUpdate("INSERT INTO Meta_Tables VALUES(1, 'EXAMPLE_TABLE', '((89))', 'N', '', 'EXAMPLE_TABLE', 1)");

      stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'Y'  ,'N'  ,'Y', 1  ,1  ,'testPLUGIN_METHOD_NAME1'  ,'testVERSION_NUMBER1'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1, "
        + "'testPAR_NAME1'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT1'  ,'testMETHOD_PARAMETER1' )");

      stmt.executeUpdate("INSERT INTO Meta_fk_table_joints VALUES( '((89))'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1 )");

      stmt.executeUpdate("INSERT INTO Meta_Source_Tables (TRANSFER_ACTION_ID, TABLE_ID,USE_TR_DATE_IN_WHERE_FLAG , COLLECTION_SET_ID , COLLECTION_ID , CONNECTION_ID, "
        + "DISTINCT_FLAG, AS_SELECT_OPTIONS, AS_SELECT_TABLESPACE, VERSION_NUMBER , TIMESTAMP_COLUMN_ID)" +
        " VALUES(1, 1, 'Y', 1, 1, 1, 'Y', 'AS_SELECT_OPTIONS', 'AS_SELECT_TABLESPACE', '1', 1)");

      stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'testCOLUMN_NAME'  ,'testCOLUMN_ALIAS_NAME'  ,'testCOLUMN_TYPE', "
        + "1  ,'Y'  ,'((89))'  ,1  ,1 )");

      stmt.executeUpdate("INSERT INTO Meta_Parameter_tables VALUES('testPAR_NAME1', '1', 'testVERSION_NUMBER1')");

      stmt.executeUpdate("INSERT INTO META_TRANSFORMATION_RULES VALUES(1 ,'TRANS_NAME' ,'CODE', 'DESCRIPTION' ,'testVERSION_NUMBER1')");

      stmt.executeUpdate("INSERT INTO Meta_versions VALUES( '((1))'  ,'testDESCRIPTION'  ,'Y'  ,'Y'  ,'testENGINE_SERVER', "
        + "'testMAIL_SERVER'  ,'testSCHEDULER_SERVER'  ,1 )");

      stmt.executeUpdate("INSERT INTO META_TRANSFORMATION_TABLES VALUES(1 ,'TRANSF_TABLE_NAME' ,'DESCRIPTION', 'testVERSION_NUMBER1' ,'Y'," +
        "1,1,1,1)");


      Statement stmt1 = dwhrep.getConnection().createStatement();

      stmt1.executeUpdate("INSERT INTO DWHType (TECHPACK_NAME,TYPENAME,TABLELEVEL,STORAGEID,PARTITIONSIZE,PARTITIONCOUNT,STATUS,TYPE," +
        "OWNER,VIEWTEMPLATE,CREATETEMPLATE,BASETABLENAME,DATADATECOLUMN,PUBLICVIEWTEMPLATE,PARTITIONPLAN) " +
        "VALUES('techpackname', 'typename', 'tablelevel', 'storageid', 1, 1, 'status', 'type', 'owner', 'viewtemplate', " +
        "'createtemplate', 'Example_table', 'datadatecolumn', 'publicviewtemplate', 'partitionplan')");

      stmt1.executeUpdate("INSERT INTO Dwhcolumn (STORAGEID,DATANAME,COLNUMBER,DATATYPE,DATASIZE,DATASCALE," +
        "UNIQUEVALUE,NULLABLE,INDEXES,UNIQUEKEY,STATUS,INCLUDESQL) " +
        "VALUES ('storageid','DATANAME',1,'DATATYPE',1,1,1,1,'INDEXES',1,'STATUS',1)");
      stmt1.executeUpdate("INSERT INTO Dwhcolumn values('storageid','DATETIME_ID',14,'datetime',0,0,255,1,'HG',1,'ENABLED',1)");

      saStmt = getRockFactory(Schema.dc).getConnection().createStatement();


      saStmt.execute("CREATE TABLE Example_table_01 (DUPLICATE VARCHAR(31))");


    } catch (SQLException sqlE) {
      System.out.println("SQLException :" + sqlE);
    }

    metaTransferActions = new Meta_transfer_actions(etlrep);
    metaTransferActions.setAction_contents("INSERT INTO Example_table_01 VALUES ('test')");
    metaVersions = new Meta_versions(etlrep);
    collection = new Meta_collections(etlrep);
    connectionPool = new ConnectionPool(etlrep);


    final List<String> tables = new ArrayList<String>();
    tables.add("Example_table_01");
    sctx.put("tableList", tables);
  }

  @Before
  public void before() throws Exception {
    objUnderTest = new DuplicateCheckAction(metaVersions, collectionSetId, collection, transferActionId,
      transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, sctx);
  }


  @After
  public void after() throws Exception {
    objUnderTest = null;
  }


  @Test
  public void testConstructor() throws EngineMetaDataException {
    try {
      assertNotNull(objUnderTest);
    } catch (Exception e) {
      System.out.println("Exception in Constructor: " + e);
    }
  }


  @Test
  public void testExecute() throws Exception {
    objUnderTest.execute();
    try {
      ResultSet result = saStmt.executeQuery("SELECT * FROM Example_table_01");
      if (result.next()) {
        String value = result.getString("DUPLICATE");
        assertEquals("test", value);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("testDuplicateCheckAction_testExecute() failed");
    }
  }

  // Start Junit for TR HO25609
  @Test
  public void testExecuteDateTimeID() throws Exception {
    objUnderTest = null;
    final AtomicBoolean columnRemoved = new AtomicBoolean(true);
    objUnderTest =new DuplicateCheckAction(metaVersions, collectionSetId, collection, transferActionId,
          transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, sctx){
      @Override
      protected void markDuplicates(final String rawTableName, final List<Dwhcolumn> columns,
                                    final String template) throws EngineException {
        for (Dwhcolumn column : columns) {
          if(column.getDataname().equals("DATETIME_ID")){
            columnRemoved.set(false);
            break;
          }
        }
      }
    };
    objUnderTest.execute();
    assertTrue("DateTime_ID not removed!!", columnRemoved.get());
  }

  @Test
  public void testGetConnections_Standalone() throws NoSuchFieldException, IllegalAccessException, EngineMetaDataException {
    {
      final Field dwhrepRockFactory = objUnderTest.getClass().getDeclaredField("dwhrepRockFactory");
      dwhrepRockFactory.setAccessible(true);
      final RockFactory dwhrepRock = (RockFactory) dwhrepRockFactory.get(objUnderTest);
      assertEquals("DWHREP connection was created to the wrong database schema", "dwhrep", dwhrepRock.getUserName());
    }

    {
      final Field dwhRockFactory = objUnderTest.getClass().getDeclaredField("dwhRockFactory");
      dwhRockFactory.setAccessible(true);
      final RockFactory dwhdbTest = (RockFactory) dwhRockFactory.get(objUnderTest);
      assertEquals("DWHDB connection was created to the wrong database schema", "dc", dwhdbTest.getUserName());
    }
  }

  @Test
  public void testGetConnections_Multiblade() throws NoSuchFieldException, IllegalAccessException, EngineMetaDataException, SQLException {
    objUnderTest = null;
    final long oldConnectionId = connectId;
    connectId = 999L;
    try {
      final Statement stmt = etlrep.getConnection().createStatement();
      stmt.executeUpdate("" +
        "INSERT\n" +
        "INTO\n" +
        "    META_DATABASES\n" +
        "    (\n" +
        "        USERNAME,\n" +
        "        TYPE_NAME,\n" +
        "        CONNECTION_ID,\n" +
        "        CONNECTION_NAME,\n" +
        "        CONNECTION_STRING,\n" +
        "        PASSWORD,\n" +
        "        DRIVER_NAME," +
        "        VERSION_NUMBER,\n" +
        "        DESCRIPTION\n" +
        "    )\n" +
        "    VALUES\n" +
        "    (\n" +
        "        'SA',\n" +
        "        'USER',\n" +
        "        " + connectId + ",\n" +
        "        'dwh_reader_1',\n" +
        "        '" + etlrep.getDbURL() + "',\n" +
        "        '',\n" +
        "        '" + etlrep.getDriverName() + "',\n" +
        "        '0',\n" +
        "        'dwh_reader_1'\n" +
        "    );");
      stmt.close();


      objUnderTest = new DuplicateCheckAction(metaVersions, collectionSetId, collection, transferActionId,
        transferBatchId, connectId, etlrep, connectionPool, metaTransferActions, sctx);

      {
        final Field dwhrepRockFactory = objUnderTest.getClass().getDeclaredField("dwhrepRockFactory");
        dwhrepRockFactory.setAccessible(true);
        final RockFactory dwhrepRock = (RockFactory) dwhrepRockFactory.get(objUnderTest);
        assertEquals("DWHREP connection was created to the wrong database schema", "dwhrep", dwhrepRock.getUserName());
      }

      {
        final Field dwhRockFactory = objUnderTest.getClass().getDeclaredField("dwhRockFactory");
        dwhRockFactory.setAccessible(true);
        final RockFactory dwhdbTest = (RockFactory) dwhRockFactory.get(objUnderTest);
        assertEquals("DWHDB connection was created to the wrong database", "SA", dwhdbTest.getUserName());
      }
    } finally {
      connectId = oldConnectionId;
    }
  }
}
