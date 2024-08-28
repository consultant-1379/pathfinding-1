package com.distocraft.dc5000.repository.cache;

import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import java.sql.SQLException;
import java.util.List;
import org.hsqldb.util.DatabaseManagerSwing;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DataFormatCacheTest {

  private static RockFactory rock = null;
  private static final String defaultDwhrepSchema = "dwhrep.";

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(defaultDwhrepSchema, "");
    rock = DatabaseTestUtils.getTestDbConnection();
    DatabaseTestUtils.loadSetup(rock, "dataFormatCache");
    DataFormatCache.initialize(rock);
  }

  @AfterClass
  public static void afterClass() {
    System.setProperty(defaultDwhrepSchema, defaultDwhrepSchema);
    DatabaseTestUtils.close(rock);
  }

  /*
    * This test is designed to test that the DataFormatCache
    * doesn't exclude the first item from the database.
    * This test was motivated by TR: HL94231
    */
  @Test
  public void testRevalidate1_Stats() {
    final String interfaceName = "INTF_DC_E_STN";
    final String tagID = "STN";
    final int numberOfExpectedItems = 26;
    final DataFormatCache dataFormatCache = DataFormatCache.getCache();
    final DFormat dFormat = dataFormatCache.getFormatWithTagID(interfaceName, tagID);
    final int numberOfActualItems = dFormat.getDItemCount();
    assertEquals(numberOfExpectedItems, numberOfActualItems);
  }

  @Test
  public void testRevalidate2_Stats() {
    final String interfaceName = "INTF_DC_E_STN";
    final String tagID = "STN";
    final int numberOfExpectedItems = 26;
    DataFormatCache.initialize(rock, DatabaseTestUtils.getTestDbUrl(), DatabaseTestUtils.getTestDbUser(),
      DatabaseTestUtils.getTestDbPassword());
    final DataFormatCache dataFormatCache = DataFormatCache.getCache();
    final DFormat dFormat = dataFormatCache.getFormatWithTagID(interfaceName, tagID);
    final int numberOfActualItems = dFormat.getDItemCount();
    assertEquals(numberOfExpectedItems, numberOfActualItems);
  }

  @Test
  public void test_getFormatWithFolderName_Stats(){
    final DFormat dFormat = DataFormatCache.getCache().getFormatWithFolderName("DC_E_STN_STN");
    assertNotNull("A DFormat should have been found", dFormat);
    assertEquals("Wrong DFormat found", "DC_E_STN:((6)):DC_E_STN_STN:mdc", dFormat.getDataFormatID());
  }

  @Test
  public void test_getFormatWithFolderName_Events(){
    final DFormat dFormat = DataFormatCache.getCache().getFormatWithFolderName("DIM_E_SGEH_SGSN");
    assertNotNull("A DFormat should have been found", dFormat);
    assertEquals("Wrong DFormat found", "DIM_E_SGEH_SGSN", dFormat.getDataFormatID());
  }

  @Test
  public void test_getFormatWithFolderName_PREV(){
	final DFormat dFormat = DataFormatCache.getCache().getFormatWithFolderName("DC_E_STN_STN_PREV");
    assertNotNull("A DFormat should have been found", dFormat);
    assertEquals("Wrong DFormat found", "DC_E_STN:((6)):DC_E_STN_STN:mdc", dFormat.getDataFormatID());
  }
  
  @Test
  public void test_getFormatWithFormatID_Stats(){
    final List<DFormat> list = DataFormatCache.getCache().getFormatWithFormatID(
      "INTF_DC_E_STN", "DC_E_STN:((6)):DC_E_STN_STN:mdc");
    assertNotNull("Returned list should not be null", list);
    assertEquals("Returned list size is incorrect", 1, list.size());
    assertEquals("DFormat.DataItem size is incorrect", 26, list.get(0).getDitems().size());
  }

  @Test
  public void test_isAnInterface_Stats(){
    boolean bool = DataFormatCache.getCache().isAnInterface("INTF_DC_E_STN");
    assertTrue("INTF_DC_E_STN should be seen as an Interface", bool);
    bool = DataFormatCache.getCache().isAnInterface("DC_E_STN_STN");
    assertFalse("DC_E_STN_STN should not be seen as an Interface", bool);
  }

  @Test
  public void test_getInterfaceNames_Stats(){
    final List interfaces = DataFormatCache.getCache().getInterfaceNames();
    assertTrue("Interface INTF_DC_E_STN not found", interfaces.contains("INTF_DC_E_STN"));
    assertFalse("DC_E_STN_STN listed as an Interface", interfaces.contains("DC_E_STN_STN"));
  }

  @Test
  public void test_GetCurrentDc_Events() {
    final String folderName = "DIM_E_SGEH_SGSN";
    final DataFormatCache cache = DataFormatCache.getCache();
    final String folderNameDc = folderName + "_CURRENT_DC";
    final DFormat dFormat = cache.getFormatWithFolderName(folderNameDc);
    assertNotNull("DFormat should have been found for " + folderNameDc, dFormat);
    assertEquals(folderName, dFormat.getFolderName());
  }

  @Test
  public void testUndefinedFormat() {
    final DataFormatCache cache = DataFormatCache.getCache();
    final DFormat dFormat = cache.getFormatWithFolderName("something");
    assertNull("Nothing should have bee found!", dFormat);
  }
}
