package com.distocraft.dc5000.etl.engine.sql;

import static org.junit.Assert.*;

import java.io.*;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.*;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.*;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.*;
import com.distocraft.dc5000.repository.cache.*;
import com.ericsson.eniq.common.CommonUtils;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;

public class LogSessionLoaderTest extends UnitDatabaseTestCase {

    private static final String TYPE_NAME = "/session/LOADER";
    private static final String LOAD_TABLE = "LOG_SESSION_LOADER";
    private static RockFactory etlrep = null;
    static RockFactory dwhrep;

    private static Meta_versions meta_versions = null;
    private static Meta_collections meta_collections = null;
    private static Meta_transfer_actions meta_transfer_actions = null;

    private static final long COLLECTION_SET_ID = 1;
    private static final long TRANSFER_ACTION_ID = 3;
    private static final long TRANSFER_BATCH_ID = 789;
    private static Long CONNECTION_ID = null;

    private static ConnectionPool connection_pool = null;
    private static SetContext set_context = null;

    private static Logger logger = Logger.getAnonymousLogger();

    private static final File baseDir = new File(System.getProperty("java.io.tmpdir"), "LogSessionLoader");

    private static final File NIQ_INI = new File(baseDir, "niq.ini");

    private static Logger log;

    private static String today = null;

    private static final String raw_regression_relative = TYPE_NAME.toLowerCase();
    private static final File raw_regression = new File(baseDir, raw_regression_relative);

    static Meta_tables metaTables;

    static Meta_columns objMetaCol;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("ETLDATA_DIR", baseDir.getPath());
        System.setProperty("EVENTS_ETLDATA_DIR", baseDir.getPath());
        System.setProperty("CONF_DIR", baseDir.getPath());
        raw_regression.deleteOnExit();

        setup(TestType.unit); // Need to change
        etlrep = getRockFactory(Schema.etlrep);

        meta_versions = new Meta_versions(etlrep);
        meta_versions.setVersion_number("0");
        connection_pool = new ConnectionPool(etlrep);
        dwhrep = getRockFactory(Schema.dwhrep);

        // Date Settings for dwhpartition
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date newDate = new Date();
        int oneDay = 1000 * 60 * 60 * 24;
        today = sdf.format(newDate);
        String yesterday = sdf.format(newDate.getTime() - oneDay);

        final Statement stmt = etlrep.getConnection().createStatement();
        Statement stmt1 = dwhrep.getConnection().createStatement();
        try {
            stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES(1, 'DC_E_MGW', 'description', '((1))', 'Y', 'type')");

            stmt.executeUpdate("INSERT INTO Meta_columns VALUES( 1  ,'COLLECTION_SET_ID'  ,'COLLECTION_SET_ID'  ,'COL_TYPE'  ,1  ,'Y'  ,'((1))'  ,1  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_source_tables VALUES( '2000-01-01 00:00:00.0'  ,1  ,1  ,'Y', "
                    + "1  ,1  ,1  ,'Y'  ,'testAS_SELECT_OPTIONS'  ,'((1))'  ,'((1))'  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_tables VALUES( 1  ,'testTABLE_NAME'  ,'((1))'  ,'Y'  ,'testJOIN_CLAUSE', "
                    + "'testTABLES_AND_ALIASES'  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_joints VALUES( 1  ,'Y'  ,'Y'  ,'Y', "
                    + "1  ,1  ,'testPLUGIN_METHOD_NAME1'  ,'((1))'  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1  ,1, "
                    + "'testPAR_NAME'  ,1  ,1  ,'testFREE_FORMAT_TRANSFORMAT1'  ,'testMETHOD_PARAMETER1' )");

            stmt.executeUpdate("INSERT INTO Meta_parameter_tables VALUES( 'testPAR_NAME'  ,'testPAR_VALUE'  ,'((1))' )");

            stmt.executeUpdate("INSERT INTO Meta_transformation_rules VALUES( 1  ,'testTRASF'  ,'testCODE', " + "'testDESCRIPTION'  ,'((1))' )");

            stmt.executeUpdate("INSERT INTO Meta_versions VALUES( '((1))'  ,'testDESCRIPTION'  ,'Y'  ,'Y'  ,'testENGINE_SERVER', "
                    + "'testMAIL_SERVER'  ,'testSCHEDULER_SERVER'  ,1 )");

            stmt.executeUpdate("INSERT INTO Meta_transformation_tables VALUES( 1  ,'testTABLE_NAME'  ,'testDESCRIPTION', "
                    + "'((1))'  ,'Y'  ,1  ,1  ,1  ,1 )");

            stmt1.executeUpdate("INSERT INTO DWHType (TECHPACK_NAME,TYPENAME,TABLELEVEL,STORAGEID,PARTITIONSIZE,PARTITIONCOUNT,STATUS,TYPE,"
                    + "OWNER,VIEWTEMPLATE,CREATETEMPLATE,BASETABLENAME,DATADATECOLUMN,PUBLICVIEWTEMPLATE,PARTITIONPLAN) "
                    + "VALUES('DC_E_MGW', 'typename', 'tablelevel', 'LOG_SESSION_LOADER:PLAIN', 1, 1, 'ACTIVE', 'type', 'owner', 'viewtemplate', "
                    + "'createtemplate', 'LOG_SESSION_LOADER', 'datadatecolumn', 'publicviewtemplate', 'partitionplan')");

            stmt1.executeUpdate("INSERT INTO DWHPartition (STORAGEID,TABLENAME,STATUS,STARTTIME,ENDTIME,LOADORDER) VALUES('LOG_SESSION_LOADER:PLAIN','LOG_SESSION_LOADER','ACTIVE','"
                    + yesterday + " 02:41:55.0','" + today + " 22:55:55.0',1)");
            stmt1.executeUpdate("INSERT INTO TPActivation (TECHPACK_NAME,STATUS,TYPE) VALUES('DC_E_MGW','ACTIVE','type')");
            stmt1.executeUpdate("INSERT INTO TypeActivation (TECHPACK_NAME,STATUS,TYPE,TYPENAME,TABLELEVEL) VALUES('DC_E_MGW','ACTIVE','type','typename','tablelevel')");
            stmt1.executeUpdate("INSERT INTO PartitionPlan (PARTITIONPLAN,DEFAULTPARTITIONSIZE,DEFAULTSTORAGETIME,PARTITIONTYPE) VALUES('partitionplan','1',3,1)");
        } finally {
            stmt.close();
            stmt1.close();
        }

        meta_collections = new Meta_collections(etlrep);
        meta_transfer_actions = new Meta_transfer_actions(etlrep);
        meta_transfer_actions.setAction_contents_01("DC_E_RAN_RNC_RAW_01");
        meta_transfer_actions.setWhere_clause("logname=LOADER\ntablename=LOG_SESSION_LOADER"); // This line is important 
        meta_versions = new Meta_versions(etlrep);
        metaTables = new Meta_tables(etlrep);
        meta_collections = new Meta_collections(etlrep);
        objMetaCol = new Meta_columns(etlrep, 1L, "((1))", 1L, 1L);

        // Initialize properties and caches
        setupProperties();
        DataFormatCache.initialize(etlrep);
        log = Logger.getLogger("LogSessionLoaderTest");
        DataFormatCache.initialize(etlrep);
        PhysicalTableCache.initialize(etlrep);
        SessionHandler.init();
    }

    private static void setupProperties() throws Exception {
        String inputTableDir;
        Map<String, String> env = System.getenv();
        inputTableDir = env.get("WORKSPACE") + File.separator + "inputTableDir";
        Properties props = new Properties();
        props.setProperty("maxLoadClauseLength", "1000");
        props.setProperty("SessionHandling.storageFile", LogSessionLoader.class.getName());
        props.setProperty("SessionHandling.log.types", "SessionHandler");
        props.setProperty("SessionHandling.log.SessionHandler.class", AdapterLog.class.getName());
        props.setProperty("SessionHandling.log.ADAPTER.inputTableDir", inputTableDir);
        StaticProperties.giveProperties(props);
    }

    @AfterClass
    public static void afterClass() {
        meta_versions = null;
        meta_collections = null;
        meta_transfer_actions = null;
        connection_pool = null;
        set_context = null;
        logger = null;
    }

    @Before
    public void before() throws IOException {
        delete(baseDir);
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            Assert.fail("Errors creating base directory " + baseDir.getPath());
        }
        if (!raw_regression.exists() && !raw_regression.mkdirs()) {
            Assert.fail("Errors creating raw directory " + raw_regression.getPath());
        }
    }

    @After
    public void after() {
        delete(baseDir);
        NIQ_INI.delete();
    }

    private static boolean delete(final File file) {
        if (!file.exists()) {
            return true;
        }
        if (file.isDirectory()) {
            final File[] sub = file.listFiles();
            for (File sf : sub) {
                if (!delete(sf)) {
                    System.out.println("Couldn't delete directory " + sf.getPath());
                    return false;
                }
            }
        }
        if (!file.delete()) {
            System.out.println("Couldn't delete file " + file.getPath());
            return false;
        }
        return true;
    }

    private LogSessionLoader getLoader(final int sc) throws Exception {
        return new LogSessionLoader(meta_versions, COLLECTION_SET_ID, meta_collections, TRANSFER_ACTION_ID, TRANSFER_BATCH_ID, CONNECTION_ID, etlrep,
                connection_pool, meta_transfer_actions, set_context, logger) {
        };
    }

    @Test
    public void testExpandPaths() throws Exception {

        // Initialiaze the niq.ini file.
        try {
            final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));

            pw.write("[DIRECTORY_STRUCTURE]\n");
            pw.write("FileSystems=4");
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        final int mpCount = 4;
        final LogSessionLoader loader = getLoader(mpCount);

        final List<File> expectedFiles = new ArrayList<File>(mpCount);
        for (int i = 0; i < mpCount; i++) {
            String mountNumber = i <= 9 ? "0" : "";
            mountNumber += i;
            final File mpFile = new File(new File(baseDir, mountNumber), raw_regression_relative);
            if (!mpFile.exists() && !mpFile.mkdirs()) {
                Assert.fail("Errors creating raw directory " + mpFile.getPath());
            }
            expectedFiles.add(mpFile);
        }
        List<File> expanded = loader.expandEtlPaths(raw_regression);
        Assert.assertNotNull(expanded);
        Assert.assertTrue("Base file expanded to the wrong number of files", expectedFiles.size() == expanded.size());
        for (File expected : expectedFiles) {
            Assert.assertTrue("Erroneous Expected Expanded Entry!", expanded.contains(expected));
        }
    }

    @Test
    public void testExpandPaths_No_HS() throws Exception {
        final int mpCount = 0;
        final LogSessionLoader loader = getLoader(mpCount);

        final List<File> expectedFiles = new ArrayList<File>(mpCount);
        final String fileName = baseDir + File.separator + raw_regression_relative;
        final File mpFile = new File(fileName);
        mpFile.deleteOnExit();
        expectedFiles.add(mpFile);

        List<File> expanded = loader.expandEtlPaths(raw_regression);
        Assert.assertNotNull(expanded);
        Assert.assertTrue("Base file expanded to the wrong number of files", expectedFiles.size() == expanded.size());
        for (File expected : expectedFiles) {
            Assert.assertTrue("Erroneous Expected Expanded Entry!", expanded.contains(expected));
        }
    }

    @Test
    public void testContructor() throws Exception {
        final LogSessionLoader loader = getLoader(0);
        Assert.assertNotNull(loader);
    }

    @Test
    public void testGetTableToFileMap_Regression_NoFilesFound() throws Exception {
        final LogSessionLoader loader = getLoader(0);
        final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
        Assert.assertNotNull("Table->File map should not be null", tableFileMap);
        Assert.assertFalse("Table->File doesnt contain expected mapping for " + LOAD_TABLE, tableFileMap.containsKey(LOAD_TABLE));
        final List<String> loadFiles = tableFileMap.get(LOAD_TABLE);
        Assert.assertNull(loadFiles);
    }

    @Ignore
    public void testGetTableToFileMap() throws Exception {
        // Initialize the niq.ini file.
        try {
            final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));

            pw.write("[DIRECTORY_STRUCTURE]\n");
            pw.write("FileSystems=4");
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Setting the number of file systems to 4
        final int mpCount = 4;
        final List<File> expectedFiles = new ArrayList<File>(mpCount);
        for (int i = 0; i < mpCount; i++) {
            String mountNumber = i <= 9 ? "0" : "";
            mountNumber += i;
            final File mpFile = new File(new File(baseDir, mountNumber), raw_regression_relative);
            mpFile.mkdirs();
            mpFile.deleteOnExit();
            final File txtFile = new File(mpFile, "LOADER_" + mountNumber + "." + today);
            txtFile.createNewFile();
            txtFile.deleteOnExit();
            if (!mpFile.exists() && !mpFile.mkdirs()) {
                Assert.fail("Errors creating raw directory " + mpFile.getPath());
            }
            if (!txtFile.exists()) {
                Assert.fail("Errors creating file" + txtFile.getPath());
            }
            expectedFiles.add(txtFile);
        }
        final LogSessionLoader loader = getLoader(mpCount);
        final Map<String, List<String>> tableFileMap = loader.getTableToFileMap();
        Assert.assertNotNull("Table->File map should not be null", tableFileMap);
        Assert.assertTrue("Table->File doesnt contain expected mapping for " + LOAD_TABLE, tableFileMap.containsKey(LOAD_TABLE));
        final List<String> loadFiles = tableFileMap.get(LOAD_TABLE);
        Assert.assertTrue("Wrong number of load file found", loadFiles.size() == 4);
    }

    @Test
    public void testGetDirectorySpreadCount_Zero() throws Exception {
        try {
            final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));
            pw.write("PriorityQueue.unremovableSetTypes=Loader");
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        final int directoryCount = CommonUtils.getNumOfDirectories(log);
        Assert.assertEquals(0, directoryCount);
    }

    @Test
    public void testGetDirectorySpreadCount() throws Exception {
        try {
            final PrintWriter pw = new PrintWriter(new FileWriter(NIQ_INI));

            pw.write("[DIRECTORY_STRUCTURE]\n");
            pw.write("FileSystems=4");
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        final int mpCount = 4;
        final int directoryCount = CommonUtils.getNumOfDirectories(log);
        Assert.assertEquals(mpCount, directoryCount);
    }

    @Test
    public void checkLoadStatement() throws Exception {
        StaticProperties.giveProperties(new Properties());

        Class.forName("org.hsqldb.jdbcDriver");
        RockFactory rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
        final Statement stmt = rockFact.getConnection().createStatement();

        stmt.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(31), COLLECTION_SET_NAME VARCHAR(31), DESCRIPTION VARCHAR(31), VERSION_NUMBER VARCHAR(31), ENABLED_FLAG VARCHAR(31), TYPE VARCHAR(31))");
        stmt.executeUpdate("INSERT INTO Meta_collection_sets VALUES ('1', 'set_name', 'description', '2', 'Y', 'type')");

        stmt.execute("CREATE TABLE Meta_databases (USERNAME varchar(30), VERSION_NUMBER varchar(32), TYPE_NAME varchar(15), CONNECTION_ID numeric(31), CONNECTION_NAME varchar(30), CONNECTION_STRING varchar(200), PASSWORD varchar(30), DESCRIPTION varchar(32000), DRIVER_NAME varchar(100), DB_LINK_NAME varchar(128))");
        stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 0, 'etlrep', 'jdbc:hsqldb:mem:testdb', '', '', 'org.hsqldb.jdbcDriver', null)");
        stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 1, 'dwhrep', 'jdbc:hsqldb:mem:testdb', '', '', 'org.hsqldb.jdbcDriver', null)");
        stmt.executeUpdate("insert into META_DATABASES VALUES ('SA', '0', 'USER', 2, 'dwh_coor', 'jdbc:hsqldb:mem:testdb', '', '', 'org.hsqldb.jdbcDriver', null)");

        stmt.execute("CREATE TABLE DWHPartition(STORAGEID varchar(12), TABLENAME varchar(12), STARTTIME timestamp, ENDTIME timestamp, STATUS varchar(12), LOADORDER int)");

        stmt.executeUpdate("insert into DWHPartition VALUES('TEST_RAW:RAW', 'TEST_RAW_00', '1970-01-01 01:00:00.0', '1970-01-01 01:00:00.0', 'ACTIVE', 3)");
        stmt.executeUpdate("insert into DWHPartition VALUES('TEST_RAW:RAW', 'TEST_RAW_01', '1970-01-01 01:00:00.0', '1970-01-01 01:00:00.0', 'ACTIVE', 2)");
        stmt.executeUpdate("insert into DWHPartition VALUES('TEST_RAW:RAW', 'TEST_RAW_02', '1970-01-01 01:00:00.0', '1970-01-01 01:00:00.0', 'ACTIVE', 1)");
        stmt.close();

        final Map<String, DFormat> it_map = new HashMap<String, DFormat>();
        final Map<String, List<DFormat>> id_map = new HashMap<String, List<DFormat>>();
        final Set<String> if_names = new HashSet<String>();
        final Map<String, DFormat> folder_map = new HashMap<String, DFormat>();

        final List<DItem> list = new ArrayList<DItem>();
        final DItem col1 = new DItem("TEST_COLUMN_1", 1, "TEST_COLUMN_1", "", "varchar", 255, 0);
        list.add(col1);
        final DItem col2 = new DItem("TEST_COLUMN_2", 2, "TEST_COLUMN_2", "", "varchar", 255, 0);
        list.add(col2);
        final DItem col3 = new DItem("TEST_COLUMN_3", 3, "TEST_COLUMN_3", "", "varchar", 255, 0);
        list.add(col3);
        final DFormat df = new DFormat("id", "tag", "dataformat", "folder", "transformer");
        df.setItems(list);

        folder_map.put("TEST_RAW", df);

        DataFormatCache.testInitialize(it_map, id_map, if_names, folder_map);
        final Meta_transfer_actions act = new Meta_transfer_actions(rockFact);
        final Properties whereProps = new Properties();
        whereProps.setProperty("tablename", "TEST_RAW");
        whereProps.setProperty("taildir", "PLAIN");
        whereProps.setProperty("fileDuplicateCheck", "true");
        whereProps.setProperty("versiondir", "0");
        act.setWhere_clause(TransferActionBase.propertiesToString(whereProps));
        final LogSessionLoader loader = new LogSessionLoader(new Meta_versions(rockFact), Long.valueOf(1), new Meta_collections(rockFact),
                Long.valueOf(1), Long.valueOf(1), Long.valueOf(1), rockFact, new ConnectionPool(rockFact), act, new SetContext(),
                Logger.getLogger("LoaderTest"));

        //Check load statement for binary
        assertEquals("LOAD TABLE $TABLE (TEST_COLUMN_1 BINARY WITH NULL BYTE, TEST_COLUMN_2 BINARY WITH NULL BYTE, "
                + "TEST_COLUMN_3 BINARY WITH NULL BYTE) FROM $FILENAMES $LOADERPARAMETERS;\n", loader.generateLoadTemplate(true));

        //Check load statement for ascii
        assertEquals("LOAD TABLE $TABLE (TEST_COLUMN_1, TEST_COLUMN_2, TEST_COLUMN_3) FROM $FILENAMES $LOADERPARAMETERS;\n",
                loader.generateLoadTemplate(false));
    }
}
