package com.ericsson.eniq.common.setWizards;

import junit.framework.TestCase;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import static org.easymock.classextension.EasyMock.anyObject;
import static org.easymock.classextension.EasyMock.createNiceMock;
import static org.easymock.classextension.EasyMock.eq;
import static org.easymock.classextension.EasyMock.expectLastCall;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import org.junit.After;
import org.junit.Before;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.net.URL;

public class CreateAggregatorSetTest extends TestCase {
    private final static String _createStatementMetaFile = "TableCreateStatements.sql";
    private static final String TEMPLATE_DIR = "5.2";
    private static final String setVersion = "((12))";
    private static final String techPackName = "DC_E_BSS";
    private static final String versionId = techPackName + ":" + setVersion;
    private static final int collectionSetId = 999;
    private static final String TESTDB_DRIVER = "org.hsqldb.jdbcDriver";
    private static final String DWHREP_URL = "jdbc:hsqldb:mem:dwhrep";
    private RockFactory dwhrep = null;
    private RockFactory etlrep = null;
    private String aggregationTypeGenerated = null;
    private String aggregationScopeGenerated = null;
    private String aggregationTemplateUsed = null;
    private static boolean velocityInit = false;
    private VelocityContext context ;

    private CreateAggregatorSet testInstance = null;

    public CreateAggregatorSetTest() {
        super("CreateAggregatorSetTest");
    }

    public void testWithNoTemplateFound() throws Exception {
        final String aggName = "qwertyuiop";
        try{
            doCreateAggregatorTest(aggName, null, null);
            fail("Exception about non existing aggregation should have been thrown");
        } catch (Exception e){
            assertTrue(e.getMessage().contains(aggName));
        }
    }

    public void testCreateMonthRankBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELLBH_MONTHRANKBH_CELL_PP5";
        final String expectedType = "RANKBH";
        final String expectedScope = "MONTH";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }
    
    public void testCreateWeekRankBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELLBH_WEEKRANKBH_CELL_PP5";
        final String expectedType = "RANKBH";
        final String expectedScope = "WEEK";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }

    public void testCreateDayTotalAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELL3_DAY";
        final String expectedType = "TOTAL";
        final String expectedScope = "DAY";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }

    public void testCreateDayBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELL3_DAYBH_Cell";
        final String expectedType = "DAYBH";
        final String expectedScope = "DAY";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }
    public void testCreateWeekBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELL3_WEEKBH_Cell";
        final String expectedType = "DAYBH";
        final String expectedScope = "WEEK";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }
    public void testCreateMonthBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELL3_MONTHBH_Cell";
        final String expectedType = "DAYBH";
        final String expectedScope = "MONTH";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }

    /**
     * Test that the correct template is used if the placeholder aggregation type is set to
     * sliding window.
     * @throws Exception If there are any errors executing the test case
     */
    public void testCreateSlidingWindowBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELLBH_RANKBH_CELL_PP4";
        final String expectedType = "RANKBH_SLIDINGWINDOW";
        final String expectedScope = "DAY";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }

    /**
     * Test that the correct template is used if the placeholder aggregation type is set to
     * a time limited window (the default type).
     * @throws Exception If there are any errors executing the test case
     */
    public void testCreateTimeLimitedBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELLBH_RANKBH_CELL_PP5";
        final String expectedType = "RANKBH_TIMELIMITED";
        final String expectedScope = "DAY";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }
    
    /**
     * Test that the correct template is used if the placeholder aggregation type is set to
     * sliding window.
     * @throws Exception If there are any errors executing the test case
     */
    public void testCreatePeakropBusyhourAggregator() throws Exception {
        final String aggType = "DC_E_BSS_CELLBH_RANKBH_CELL_PP3";
        final String expectedType = "RANKBH_PEAKROP";
        final String expectedScope = "DAY";
        doCreateAggregatorTest(aggType, expectedType, expectedScope);
    }

    private void doCreateAggregatorTest(final String aggName, final String expAggType, final String expAggScope) throws Exception {
    	context = new VelocityContext();
        context.put("table", "DummyTable");
        context.put("targetTableList", "DummyTargetTableList");
        context.put("dateid", "DummyDateID");
        testInstance.CreateAggregationSql(aggName, context);
        final String expectedTemplate = expAggType + "." + expAggScope + ".vm";
        assertEquals("Aggregation Type used to create aggregator not correct", expAggType, aggregationTypeGenerated);
        assertEquals("Aggregation Scope used to create aggregator not correct", expAggScope, aggregationScopeGenerated);
        assertEquals("Wrong template used to generate Aggregation SQL", expectedTemplate, aggregationTemplateUsed);
    }

    @Override @Before
    protected void setUp() throws Exception {
        _init_velocity();
        // etlrep is used to get the collectionset/id transfer action id, not concerned with those in these tests
        _init_mock_etlrep();

        dwhrep = new RockFactory(DWHREP_URL, "SA", "", TESTDB_DRIVER, "test", true);
        loadSetup(dwhrep, "createAggSets_BH");
        aggregationTypeGenerated = null;
        aggregationScopeGenerated = null;
        aggregationTemplateUsed = null;
        testInstance = new CreateAggregatorSet(TEMPLATE_DIR, techPackName, setVersion,
                versionId, dwhrep, etlrep, collectionSetId, false) {
            @Override
            protected String getTemplate(final String aggregationtype, final String aggregationscope) {
                aggregationTypeGenerated = aggregationtype;
                aggregationScopeGenerated = aggregationscope;
                try {
					aggregationTemplateUsed = super.getTemplate(aggregationtype, aggregationscope);
				} catch (RockException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                return aggregationTemplateUsed;
            }
        };
    }
    @Override @After
    protected void tearDown() throws Exception {
        reset(etlrep);
        final Statement stmt = dwhrep.getConnection().createStatement();
        stmt.executeUpdate("SHUTDOWN");
        stmt.close();
        dwhrep.getConnection().close();
        dwhrep = null;
        testInstance = null;
    }
    private void _init_velocity() throws Exception {
        if(!velocityInit){
            Velocity.setProperty("resource.loader", "class");
            Velocity.setProperty("class.resource.loader.class",
                    "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
            Velocity.init();
            velocityInit = true;
        }
    }
    private void _init_mock_etlrep() throws Exception {
        final Connection conn = createNiceMock(Connection.class);
        final Statement stmt = createNiceMock(Statement.class);
        final ResultSet rs = createNiceMock(ResultSet.class);
        etlrep = createNiceMock(RockFactory.class);
        etlrep.getConnection();
        expectLastCall().andReturn(conn);
        expectLastCall().anyTimes();

        conn.createStatement();
        expectLastCall().andReturn(stmt);
        expectLastCall().anyTimes();

        stmt.executeQuery((String) anyObject());
        expectLastCall().andReturn(rs);
        expectLastCall().anyTimes();

        rs.next();
        expectLastCall().andReturn(true);
        expectLastCall().anyTimes();

        rs.getLong(eq("maxval"));
        expectLastCall().andReturn(collectionSetId);
        expectLastCall().anyTimes();

        replay(rs);
        replay(stmt);
        replay(conn);
        replay(etlrep);
    }

    private void loadSetup(final RockFactory testDB, final String base) throws Exception {
			final String lookFor = "setupSQL/" + base;
        final URL url = ClassLoader.getSystemResource(lookFor);
			if(url == null){
				throw new Exception("Couldn't find '"+lookFor+"' on classpath.");
			}

        final String baseDir = url.toURI().getRawPath();
        final File loadFrom = new File(baseDir);
        final File[] toLoad = loadFrom.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".sql") && !name.equals(_createStatementMetaFile);
            }
        });
        final File createFile = new File(baseDir + "/" + _createStatementMetaFile);
        loadSqlFile(createFile, testDB);
        for (File loadFile : toLoad) {
            loadSqlFile(loadFile, testDB);
        }
    }
    private void loadSqlFile(final File sqlFile, final RockFactory testDB) throws IOException, SQLException, ClassNotFoundException {
        if (!sqlFile.exists()) {
            return;
        }
        BufferedReader br = new BufferedReader(new FileReader(sqlFile));
        String line;
        int lineCount = 0;
        try {
            while ((line = br.readLine()) != null) {
                lineCount++;
                line = line.trim();
                if (line.length() == 0 || line.startsWith("#")) {
                    continue;
                }
                while (!line.endsWith(";")) {
                    final String tmp = br.readLine();
                    if (tmp != null) {
                        line += "\r\n";
                        line += tmp;
                    } else {
                        break;
                    }
                }
                update(line, testDB);
            }
            testDB.commit();
        } catch (SQLException e) {
            throw new SQLException("Error executing on line [" + lineCount + "] of " + sqlFile, e);
        } finally {
            br.close();
        }
    }
    private void update(final String insertSQL, final RockFactory testDB) throws SQLException, ClassNotFoundException, IOException {
        final Statement s = testDB.getConnection().createStatement();
        try {
            _doExecuteUpdate(insertSQL, s);
        } catch (SQLException e) {
            if (e.getSQLState().equals("S0004")) {
                System.out.println("Views not supported yet: " + e.getMessage());
            } else if (e.getSQLState().equals("S0001") || e.getSQLState().equals("42504")) {
                //ignore, table already exists.......
            } else {
                throw e;
            }
        }
    }
    private void _doExecuteUpdate(final String insertSQL, final Statement s) throws SQLException {
        s.executeUpdate(insertSQL);
    }
}