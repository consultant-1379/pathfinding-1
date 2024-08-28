/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.common.setWizards;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ssc.rockfactory.RockFactory;
import utils.TestUtils;


/**
 * Test for CreateSonvRestoreSet
 * 
 * @author epaujor
 * @since 2012
 * 
 */
public class CreateSonvRestoreSetTest {

  private final static boolean isScheduling = true;

  private final static int techpackId = 123;

  private static final String SONV_RESTORE_SET_NAME = "Restore_Set";

  private static final String setVersion = "((12))";

  private static final String versionId = "EVENT_E_SGEH:((12))";

  private static RockFactory dwhrep = null;

  private static RockFactory etlrep = null;

  @Before
  public void setUp() throws Exception {
    etlrep = CreateRockFactoryHelper.createEtlRepRockFactory();
    dwhrep = CreateRockFactoryHelper.createDwhRepFactory();
    TestUtils.loadSetup(dwhrep, "createCountSets");
  }

  @After
  public void tearDown() throws Exception {
    CreateRockFactoryHelper.cleanUpRockFactory();
    etlrep = null;
    dwhrep = null;
  }

  @Test
  public void checkThatCreateAndRemoveSetsWorksForSonvRestoreSets() throws Exception {
    final CreateSonvRestoreSet createSonvRestoreSet = new CreateSonvRestoreSet(SONV_RESTORE_SET_NAME, setVersion,
        versionId, dwhrep, etlrep, techpackId, isScheduling);
    ResultSet resultSet = null;
    try {
      // META_TRANSFER_ACTIONS should be empty here
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_TRANSFER_ACTIONS");
      assertFalse(resultSet.next());

      // META_COLLECTIONS should be empty here
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_COLLECTIONS");
      assertFalse(resultSet.next());

      createSonvRestoreSet.create(false);

      // Check META_TRANSFER_ACTIONS
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_TRANSFER_ACTIONS");
      assertTrue(resultSet.next());
      checkMeasurementTypeActions(resultSet);
      checkReferenceTableActions(resultSet);
      assertFalse(resultSet.next());

      // Check META_COLLECTIONS
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_COLLECTIONS");
      assertTrue(resultSet.next());
      checkMeasurementTypeCollections(resultSet);
      checkReferenceTableCollections(resultSet);
      assertFalse(resultSet.next());

      // Check META_COLLECTIONS
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_SCHEDULINGS");
      assertFalse(resultSet.next());

      createSonvRestoreSet.removeSets();
      // META_TRANSFER_ACTIONS should be empty again after remove
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_TRANSFER_ACTIONS");
      assertFalse(resultSet.next());

      // META_COLLECTIONS should be empty again after remove
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_COLLECTIONS");
      assertFalse(resultSet.next());

      // META_SCHEDULINGS should be empty again after remove
      resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_SCHEDULINGS");
      assertFalse(resultSet.next());
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }

  /**
   * @param resultSet
   * @throws SQLException
   */
  private void checkReferenceTableCollections(final ResultSet resultSet) throws SQLException {
    assertTrue(resultSet.next());
    assertEquals("5", resultSet.getString(1));
    assertEquals("Restore_Count_TEST_TABLE_PLAIN", resultSet.getString(2));
    assertEquals(null, resultSet.getString(3));
    assertEquals(null, resultSet.getString(4));
    assertEquals(null, resultSet.getString(5));
    assertEquals(null, resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals("0", resultSet.getString(8));
    assertEquals("0", resultSet.getString(9));
    assertEquals("N", resultSet.getString(10));
    assertEquals("N", resultSet.getString(11));
    assertEquals(null, resultSet.getString(12));
    assertEquals("((12))", resultSet.getString(13));
    assertEquals("123", resultSet.getString(14));
    assertEquals(null, resultSet.getString(15));
    assertEquals("0", resultSet.getString(16));
    assertEquals("72000", resultSet.getString(17));
    assertEquals("Y", resultSet.getString(18));
    assertEquals("Backup", resultSet.getString(19));
    assertEquals("Y", resultSet.getString(20));
    assertEquals(null, resultSet.getString(21));
    assertEquals("N", resultSet.getString(22));
    assertEquals(null, resultSet.getString(23));
    
    assertTrue(resultSet.next());
    assertEquals("6", resultSet.getString(1));
    assertEquals("Restore_Count_TEST_TABLE_HIST_RAW_PLAIN", resultSet.getString(2));
    assertEquals(null, resultSet.getString(3));
    assertEquals(null, resultSet.getString(4));
    assertEquals(null, resultSet.getString(5));
    assertEquals(null, resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals("0", resultSet.getString(8));
    assertEquals("0", resultSet.getString(9));
    assertEquals("N", resultSet.getString(10));
    assertEquals("N", resultSet.getString(11));
    assertEquals(null, resultSet.getString(12));
    assertEquals("((12))", resultSet.getString(13));
    assertEquals("123", resultSet.getString(14));
    assertEquals(null, resultSet.getString(15));
    assertEquals("0", resultSet.getString(16));
    assertEquals("72000", resultSet.getString(17));
    assertEquals("Y", resultSet.getString(18));
    assertEquals("Backup", resultSet.getString(19));
    assertEquals("Y", resultSet.getString(20));
    assertEquals(null, resultSet.getString(21));
    assertEquals("N", resultSet.getString(22));
    assertEquals(null, resultSet.getString(23));
  }

  /**
   * @param resultSet
   * @throws SQLException
   */
  private void checkMeasurementTypeCollections(final ResultSet resultSet) throws SQLException {
    assertEquals("1", resultSet.getString(1));
    assertEquals("Restore_Count_EVENT_E_SGEH_BSC_RAW", resultSet.getString(2));
    assertEquals(null, resultSet.getString(3));
    assertEquals(null, resultSet.getString(4));
    assertEquals(null, resultSet.getString(5));
    assertEquals(null, resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals("0", resultSet.getString(8));
    assertEquals("0", resultSet.getString(9));
    assertEquals("N", resultSet.getString(10));
    assertEquals("N", resultSet.getString(11));
    assertEquals(null, resultSet.getString(12));
    assertEquals("((12))", resultSet.getString(13));
    assertEquals("123", resultSet.getString(14));
    assertEquals(null, resultSet.getString(15));
    assertEquals("0", resultSet.getString(16));
    assertEquals("72000", resultSet.getString(17));
    assertEquals("Y", resultSet.getString(18));
    assertEquals("Backup", resultSet.getString(19));
    assertEquals("Y", resultSet.getString(20));
    assertEquals(null, resultSet.getString(21));
    assertEquals("N", resultSet.getString(22));
    assertEquals(null, resultSet.getString(23));

    assertTrue(resultSet.next());
    assertEquals("2", resultSet.getString(1));
    assertEquals("Restore_Count_EVENT_E_SGEH_CELL3_DAY", resultSet.getString(2));
    assertEquals(null, resultSet.getString(3));
    assertEquals(null, resultSet.getString(4));
    assertEquals(null, resultSet.getString(5));
    assertEquals(null, resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals("0", resultSet.getString(8));
    assertEquals("0", resultSet.getString(9));
    assertEquals("N", resultSet.getString(10));
    assertEquals("N", resultSet.getString(11));
    assertEquals(null, resultSet.getString(12));
    assertEquals("((12))", resultSet.getString(13));
    assertEquals("123", resultSet.getString(14));
    assertEquals(null, resultSet.getString(15));
    assertEquals("0", resultSet.getString(16));
    assertEquals("72000", resultSet.getString(17));
    assertEquals("Y", resultSet.getString(18));
    assertEquals("Backup", resultSet.getString(19));
    assertEquals("Y", resultSet.getString(20));
    assertEquals(null, resultSet.getString(21));
    assertEquals("N", resultSet.getString(22));
    assertEquals(null, resultSet.getString(23));

    assertTrue(resultSet.next());
    assertEquals("3", resultSet.getString(1));
    assertEquals("Restore_Count_EVENT_E_SGEH_CELL3_RAW", resultSet.getString(2));
    assertEquals(null, resultSet.getString(3));
    assertEquals(null, resultSet.getString(4));
    assertEquals(null, resultSet.getString(5));
    assertEquals(null, resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals("0", resultSet.getString(8));
    assertEquals("0", resultSet.getString(9));
    assertEquals("N", resultSet.getString(10));
    assertEquals("N", resultSet.getString(11));
    assertEquals(null, resultSet.getString(12));
    assertEquals("((12))", resultSet.getString(13));
    assertEquals("123", resultSet.getString(14));
    assertEquals(null, resultSet.getString(15));
    assertEquals("0", resultSet.getString(16));
    assertEquals("72000", resultSet.getString(17));
    assertEquals("Y", resultSet.getString(18));
    assertEquals("Backup", resultSet.getString(19));
    assertEquals("Y", resultSet.getString(20));
    assertEquals(null, resultSet.getString(21));
    assertEquals("N", resultSet.getString(22));
    assertEquals(null, resultSet.getString(23));

    assertTrue(resultSet.next());
    assertEquals("4", resultSet.getString(1));
    assertEquals("Restore_Count_EVENT_E_SGEH_CELL_15MIN", resultSet.getString(2));
    assertEquals(null, resultSet.getString(3));
    assertEquals(null, resultSet.getString(4));
    assertEquals(null, resultSet.getString(5));
    assertEquals(null, resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals("0", resultSet.getString(8));
    assertEquals("0", resultSet.getString(9));
    assertEquals("N", resultSet.getString(10));
    assertEquals("N", resultSet.getString(11));
    assertEquals(null, resultSet.getString(12));
    assertEquals("((12))", resultSet.getString(13));
    assertEquals("123", resultSet.getString(14));
    assertEquals(null, resultSet.getString(15));
    assertEquals("0", resultSet.getString(16));
    assertEquals("72000", resultSet.getString(17));
    assertEquals("Y", resultSet.getString(18));
    assertEquals("Backup", resultSet.getString(19));
    assertEquals("Y", resultSet.getString(20));
    assertEquals(null, resultSet.getString(21));
    assertEquals("N", resultSet.getString(22));
    assertEquals(null, resultSet.getString(23));
  }

  /**
   * @param resultSet
   * @throws SQLException
   */
  private void checkReferenceTableActions(final ResultSet resultSet) throws SQLException {
    // Check actions have been created for reference tables
    // Each set for a reference tables should contain a "RestoreTables" action
    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("8", resultSet.getString(2));
    assertEquals("5", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("Restore", resultSet.getString(5));
    assertEquals("Restore_Count_TEST_TABLE_PLAIN", resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertTrue(resultSet.getString(9).contains("typeName=TEST_TABLE_CURRENT_DC"));
    assertTrue(resultSet.getString(9).contains("tablename=TEST_TABLE\\:PLAIN"));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));
    
    // eeoidiv,20121203:History Dynamic
    // If ReferenceTable.UPDATE_POLICY==4 (History Dynamic) then additional History Table backup set.
    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("9", resultSet.getString(2));
    assertEquals("6", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("Restore", resultSet.getString(5));
    assertEquals("Restore_Count_TEST_TABLE_HIST_RAW_PLAIN", resultSet.getString(6));
    assertEquals("1", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertTrue(resultSet.getString(9).contains("typeName=TEST_TABLE_HIST_RAW_CURRENT_DC"));
    assertTrue(resultSet.getString(9).contains("tablename=TEST_TABLE_HIST_RAW\\:PLAIN"));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));
  }

  /**
   * @param resultSet
   * @throws SQLException
   */
  private void checkMeasurementTypeActions(final ResultSet resultSet) throws SQLException {
    // Check actions have been created for measurement types
    // Each set for a measurement type should contain a "RestoreTables" action
    // and a "StoreCountingData" action
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("1", resultSet.getString(2));
    assertEquals("1", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("Restore", resultSet.getString(5));
    assertEquals("Restore_Count_EVENT_E_SGEH_BSC_RAW", resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertTrue(resultSet.getString(9).contains("typeName=EVENT_E_SGEH_BSC_CURRENT_DC"));
    assertTrue(resultSet.getString(9).contains("tablename=EVENT_E_SGEH_BSC\\:RAW"));
    assertTrue(resultSet.getString(9).contains("isVolBasedPartition=true"));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));

    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("2", resultSet.getString(2));
    assertEquals("1", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("StoreCountingData", resultSet.getString(5));
    assertEquals("Store_EVENT_E_SGEH_BSC_RAW", resultSet.getString(6));
    assertEquals("1", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertEquals("", resultSet.getString(9));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));

    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("3", resultSet.getString(2));
    assertEquals("2", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("Restore", resultSet.getString(5));
    assertEquals("Restore_Count_EVENT_E_SGEH_CELL3_DAY", resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertTrue(resultSet.getString(9).contains("typeName=EVENT_E_SGEH_CELL3_CURRENT_DC"));
    assertTrue(resultSet.getString(9).contains("tablename=EVENT_E_SGEH_CELL3\\:DAY"));
    assertTrue(resultSet.getString(9).contains("isVolBasedPartition=true"));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));

    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("4", resultSet.getString(2));
    assertEquals("2", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("StoreCountingData", resultSet.getString(5));
    assertEquals("Store_EVENT_E_SGEH_CELL3_DAY", resultSet.getString(6));
    assertEquals("1", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertEquals("", resultSet.getString(9));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));

    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("5", resultSet.getString(2));
    assertEquals("3", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("Restore", resultSet.getString(5));
    assertEquals("Restore_Count_EVENT_E_SGEH_CELL3_RAW", resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertTrue(resultSet.getString(9).contains("typeName=EVENT_E_SGEH_CELL3_CURRENT_DC"));
    assertTrue(resultSet.getString(9).contains("tablename=EVENT_E_SGEH_CELL3\\:RAW"));
    assertTrue(resultSet.getString(9).contains("isVolBasedPartition=false"));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));

    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("6", resultSet.getString(2));
    assertEquals("4", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("Restore", resultSet.getString(5));
    assertEquals("Restore_Count_EVENT_E_SGEH_CELL_15MIN", resultSet.getString(6));
    assertEquals("0", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertTrue(resultSet.getString(9).contains("typeName=EVENT_E_SGEH_CELL_CURRENT_DC"));
    assertTrue(resultSet.getString(9).contains("tablename=EVENT_E_SGEH_CELL\\:15MIN"));
    assertTrue(resultSet.getString(9).contains("isVolBasedPartition=true"));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));

    assertTrue(resultSet.next());
    assertEquals("((12))", resultSet.getString(1));
    assertEquals("7", resultSet.getString(2));
    assertEquals("4", resultSet.getString(3));
    assertEquals("123", resultSet.getString(4));
    assertEquals("StoreCountingData", resultSet.getString(5));
    assertEquals("Store_EVENT_E_SGEH_CELL_15MIN", resultSet.getString(6));
    assertEquals("1", resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertEquals("", resultSet.getString(9));
    assertEquals("", resultSet.getString(10));
    assertEquals("Y", resultSet.getString(11));
    assertEquals("2", resultSet.getString(12));
    assertEquals("", resultSet.getString(13));
    assertEquals("", resultSet.getString(14));
    assertEquals("", resultSet.getString(15));
    assertEquals("", resultSet.getString(16));
  }
}
