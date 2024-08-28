package com.distocraft.dc5000.etl.engine.sql;

import java.io.File;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.repository.cache.DFormat;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import com.ericsson.eniq.exception.ConfigurationException;
import com.ericsson.eniq.exception.FileException;

public class LoaderTest {
  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "LoaderTest");
	
  // Mock context:
  protected Mockery context = new JUnit4Mockery();

  {
    // we need to mock classes, not just interfaces.
    context.setImposteriser(ClassImposteriser.INSTANCE);
  }

  private Loader testInstance;

  // Mocks:
  private DataFormatCache mockDataformatCache;
   
  private DFormat mockDFormat;
    
  private Logger mockLog;

  private final String TEST_TABLE_NAME = "LOG_SESSION_ADAPTER";

  RockFactory rockFactory;

  @BeforeClass
  public static void beforeClass(){
    if(!TMP_DIR.exists() && !TMP_DIR.mkdirs()){
      Assert.fail("Failed to create " + TMP_DIR.getPath());
    }
  }

  @AfterClass
  public static void afterClass(){
    DirectoryHelper.delete(TMP_DIR);
  }

  @Before
  public void setUp() {

    mockDataformatCache = context.mock(DataFormatCache.class);
    mockDFormat = context.mock(DFormat.class);
    mockLog = context.mock(Logger.class);

    
    try {
		rockFactory = new RockFactory("jdbc:hsqldb:mem:testdb", "sa", "", "org.hsqldb.jdbcDriver", "con", true);
	} catch (Exception e) {
		e.printStackTrace();
	}
    
    // Set up a child class of loader:
    testInstance = new Loader() {

      @Override
      protected void initializeLoggers() {
        super.log = mockLog;
      }

      @Override
      protected Map<String, List<String>> getTableToFileMap() throws ConfigurationException, FileException,
          RockException, SQLException {
        return null;
      }

      @Override
      protected void fillVelocityContext(VelocityContext context) {

      }

      @Override
      protected void updateSessionLog() {

      }

      @Override
      protected DataFormatCache getDataformatCache() {
        return mockDataformatCache;
      }
    };

    // Set up the logger:
    testInstance.initializeLoggers();

  }

  @After
  public void tearDown() {
    testInstance = null;
  }

  /**
   * Test setting up data format for DWH Monitor. Should return null, none
   * defined.
   */
  @Test
  public void testSetupDataformatForDWHMonitor() {
    // Expectations: DWH_MONITOR will return null because there are no data
    // formats defined.
    context.checking(new Expectations() {

      {
        oneOf(mockDataformatCache).getFormatWithFolderName(TEST_TABLE_NAME);
        will(returnValue(null));

        allowing(mockLog);
      }
    });

    testInstance.setTpName("DWH_MONITOR");
    final DFormat dataFormat = testInstance.setupDataformat(TEST_TABLE_NAME);
    Assert.assertNull("Data format should be null for DWH MONITOR, no data formats defined", dataFormat);
  }

  /**
   * Test setting up data format for table like SELECT_RAN_CELL. Should return null, none defined.
   */
  @Test
  public void testSetupDataformat() {
    // Expectations: SELECT_RAN_CELL will return null because there are no data
    // formats defined.
    context.checking(new Expectations() {

      {
        oneOf(mockDataformatCache).getFormatWithFolderName("SELECT_RAN_CELL");
        will(returnValue(null));

        oneOf(mockDataformatCache).getFormatWithFolderName("SELECT_LTE_CELL");
        will(returnValue(null));
        
        oneOf(mockDataformatCache).getFormatWithFolderName("DIM_X_BSS_CELL_CURRENT_DC");
        will(returnValue(null));
        
        allowing(mockLog);
      }
    });

    testInstance.setTpName("UTRAN_BASE");
    final DFormat dataFormat1 = testInstance.setupDataformat("SELECT_RAN_CELL");
    Assert.assertEquals(null, dataFormat1);   
    
    testInstance.setTpName("LTE_BASE");
    final DFormat dataFormat2 = testInstance.setupDataformat("SELECT_LTE_CELL");
    Assert.assertEquals(null, dataFormat2);
    
    testInstance.setTpName("DC_BSS_BASE");
    final DFormat dataFormat3 = testInstance.setupDataformat("DIM_X_BSS_CELL_CURRENT_DC");
    Assert.assertEquals(null, dataFormat3);
  }
  
  /**
   * Test setting up data format for normal tech pack. Should not return null.
   */
  @Test
  public void testSetupDataformatForTechPack() {
    // Expectations: DC_E_MGW will not return null for data format.
    context.checking(new Expectations() {

      {
        oneOf(mockDataformatCache).getFormatWithFolderName("DC_E_MGW_AAL2SP");
        will(returnValue(mockDFormat));

        oneOf(mockDFormat).getDataFormatID();
        will(returnValue("DC_E_MGW:((23)):DC_E_MGW_AAL2SP:mdc"));

        // Ten data items:
        oneOf(mockDFormat).getDItemCount();
        will(returnValue(10));

        allowing(mockLog);
      }
    });

    testInstance.setTpName("DC_E_MGW");
    final DFormat dataFormat = testInstance.setupDataformat("DC_E_MGW_AAL2SP");
    Assert.assertNotNull("Data format should not be null for normal tech pack", dataFormat);
  }

  /**
   * Test setting up data format for normal tech pack. Tests the situation where
   * DataformatCache returns a null value when it's called.
   */
  @Test
  public void testSetupDataformatNullFormatReturned() {
    // Expectations: A null dataformat will be returned. Method should not
    // crash.
    context.checking(new Expectations() {

      {
        oneOf(mockDataformatCache).getFormatWithFolderName("DC_E_MGW_AAL2SP");
        will(returnValue(null));

        allowing(mockLog);
      }
    });

    try {
      testInstance.setTpName("DC_E_MGW");
      final DFormat dataFormat = testInstance.setupDataformat("DC_E_MGW_AAL2SP");
    } catch (Exception exc) {
      // If there is an exception, this test has failed:
      Assert.fail("Null value was returned by DataformatCache but was not handled correctly by setupDataformat");
    }
  }
 
  public void testformatFileNamesForLog() {
	  try {
		  List li = new ArrayList();
		  li.add("file1");
		  String str = testInstance.formatFileNamesForLog(li, "true");
		  String expected = "\n" + "file1true" + "\n";
      Assert.assertEquals(expected, str);
	  } catch (Exception exc) {
	    System.out.println("Exception : " + exc);
	  }
  }


  @Test
  public void test_getAbsoluteFailedDir(){


    testInstance.failedDirName = "failed";
    testInstance.tablename = "dc_e_abc";


    String loadFile = "/eniq/data/etldata/"+testInstance.tablename+"/raw/load_file.sql";
    String expectedFailedDir = "/eniq/data/etldata/"+testInstance.tablename+"/" + testInstance.failedDirName;
    System.setProperty("ETLDATA_DIR", "/eniq/data/etldata");
    String failedDir = testInstance.getAbsoluteFailedDir(loadFile);

    Assert.assertEquals("Failed directory not calculated correctly", new File(expectedFailedDir) , new File(failedDir));

    loadFile = "/eniq/data/etldata/06/"+testInstance.tablename+"/raw/load_file.sql";
    expectedFailedDir = "/eniq/data/etldata/06/"+testInstance.tablename+"/" + testInstance.failedDirName;
    failedDir = testInstance.getAbsoluteFailedDir(loadFile);
    Assert.assertEquals("Failed directory not calculated correctly", new File(expectedFailedDir) , new File(failedDir));
  }
 
  public void testmoveToDirectory() {
	    context.checking(new Expectations() {
	        {
	          allowing(mockLog);
	        }
	      });
	  try {
		  File f = new File("file1");
		  Boolean b = testInstance.moveToDirectory(f, TMP_DIR.getPath());
      Assert.assertFalse(b);
	  } catch (Exception exc) {
	    System.out.println("Exception : " + exc);
	  }
  }	  
  
  public void testformatFileNamesForSQL() {
	    context.checking(new Expectations() {
	        {
	          allowing(mockLog);
	        }
	      });
	  try {
		  List l = new ArrayList();
		  l.add("file1");
		  String str1 = testInstance.getFileNamesInCorrectFormat(l, 10, "test");
      Assert.assertEquals("'file1'", str1);
	  } catch (Exception exc) {
	    System.out.println("Exception : " + exc);
	  }
}	 
	
  @Test
  public void testpruneDuplicates() throws Exception {
	    context.checking(new Expectations() {
	        {
	          allowing(mockLog);
	        }
	    });
      Statement stmt = rockFactory.getConnection().createStatement();
	  List<String> l = new ArrayList<String>();
	  l.add("file1");
	  try {
	  testInstance.pruneDuplicates(l, stmt);
	  }
	  catch(Exception e){
		  
	  }
  }
  
}
