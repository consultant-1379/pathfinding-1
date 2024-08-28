package com.ericsson.eniq.common.setWizards;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.common.StaticProperties;

import ssc.rockfactory.RockFactory;

public class CreateIDiskmanagerSetTest extends TestCase {

    private final String setVersion = "((12))";

    private final int techPackID = 123;

    private String setName = null;

    private RockFactory etlrep = null;

    private String objType = "reference";

    private CreateIDiskmanagerSet createIDiskmanagerSet;

    @Override
    @Before
    public void setUp() throws Exception {
        StaticProperties.giveProperties(new Properties());
        etlrep = CreateRockFactoryHelper.createEtlRepRockFactory();
        setName = "INTF_DIM_E_LTE_SON_TEST";
        createIDiskmanagerSet = new CreateIDiskmanagerSet(objType, setVersion, etlrep, techPackID, setName, "");
    }

    @Override
    @After
    public void tearDown() throws SQLException {
        CreateRockFactoryHelper.cleanUpRockFactory();
        etlrep = null;
        createIDiskmanagerSet = null;
    }

    @Test
    public void testCreateIDiskmanagerSetWithSchedulings() throws Exception {
        // Schedulings enabled
        boolean schedulings = true;
        // Before the tests database is empty
        assertTrue(checkBeforeAndAfterStatus());

        createIDiskmanagerSet.create(true, schedulings);
        // Check the database content
        assertTrue(checkCreateIDiskmanagerSet(schedulings));

        createIDiskmanagerSet.removeSets(false);
        // After the tests database is empty
        assertTrue(checkBeforeAndAfterStatus());
    }

    @Test
    public void testCreateIDiskmanagerSetWithoutSchedulings() throws Exception {
        // Schedulings disabled
        boolean schedulings = false;
        // Before the tests database is empty
        assertTrue(checkBeforeAndAfterStatus());

        createIDiskmanagerSet.create(true, schedulings);
        // Check the database content
        assertTrue(checkCreateIDiskmanagerSet(schedulings));

        createIDiskmanagerSet.removeSets(false);
        // After the tests database is empty
        assertTrue(checkBeforeAndAfterStatus());
    }

    private boolean checkBeforeAndAfterStatus() throws Exception {
        // META_COLLECTIONS should be empty again after remove
        ResultSet resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_COLLECTIONS");
        assertFalse(resultSet.next());

        // META_TRANSFER_ACTIONS should be empty again after remove
        resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_TRANSFER_ACTIONS");
        assertFalse(resultSet.next());

        // META_SCHEDULINGS should be empty again after remove
        resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_SCHEDULINGS");
        assertFalse(resultSet.next());
        return true;
    }

    private boolean checkCreateIDiskmanagerSet(boolean schedulings) throws Exception {

        // Check META_COLLECTIONS
        ResultSet resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_COLLECTIONS");
        assertTrue(resultSet.next());

        assertEquals("1", resultSet.getString(1));
        assertEquals("Diskmanager_" + setName, resultSet.getString(2));
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
        assertEquals("3", resultSet.getString(16));
        assertEquals("30", resultSet.getString(17));
        assertEquals("Y", resultSet.getString(18));
        assertEquals("Service", resultSet.getString(19));
        assertEquals("Y", resultSet.getString(20));
        assertEquals(null, resultSet.getString(21));
        assertEquals("N", resultSet.getString(22));
        assertEquals(null, resultSet.getString(23));

        assertFalse(resultSet.next());

        // Check META_TRANSFER_ACTIONS
        resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_TRANSFER_ACTIONS");
        assertTrue(resultSet.next());

        assertEquals("((12))", resultSet.getString(1));
        assertEquals("1", resultSet.getString(2));
        assertEquals("1", resultSet.getString(3));
        assertEquals("123", resultSet.getString(4));
        assertEquals("Diskmanager", resultSet.getString(5));
        assertEquals("Diskmanager_archive", resultSet.getString(6));
        assertEquals("0", resultSet.getString(7));
        assertEquals(null, resultSet.getString(8));
        assertTrue(resultSet.getString(10).contains("diskManager.dir.fileMask=.*"));
        assertTrue(resultSet.getString(10).contains("archive"));
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
        assertEquals("Diskmanager", resultSet.getString(5));
        assertEquals("Diskmanager_processed", resultSet.getString(6));
        assertEquals("1", resultSet.getString(7));
        assertEquals(null, resultSet.getString(8));
        assertTrue(resultSet.getString(10).contains("diskManager.dir.fileMask=.*"));
        assertTrue(resultSet.getString(10).contains("processed"));
        assertEquals("Y", resultSet.getString(11));
        assertEquals("2", resultSet.getString(12));
        assertEquals("", resultSet.getString(13));
        assertEquals("", resultSet.getString(14));
        assertEquals("", resultSet.getString(15));
        assertEquals("", resultSet.getString(16));

        assertTrue(resultSet.next());
        assertEquals("((12))", resultSet.getString(1));
        assertEquals("3", resultSet.getString(2));
        assertEquals("1", resultSet.getString(3));
        assertEquals("123", resultSet.getString(4));
        assertEquals("Diskmanager", resultSet.getString(5));
        assertEquals("Diskmanager_double", resultSet.getString(6));
        assertEquals("2", resultSet.getString(7));
        assertEquals(null, resultSet.getString(8));
        assertTrue(resultSet.getString(10).contains("diskManager.dir.fileMask=.*"));
        assertTrue(resultSet.getString(10).contains("double"));
        assertEquals("Y", resultSet.getString(11));
        assertEquals("2", resultSet.getString(12));
        assertEquals("", resultSet.getString(13));
        assertEquals("", resultSet.getString(14));
        assertEquals("", resultSet.getString(15));
        assertEquals("", resultSet.getString(16));

        assertTrue(resultSet.next());
        assertEquals("((12))", resultSet.getString(1));
        assertEquals("4", resultSet.getString(2));
        assertEquals("1", resultSet.getString(3));
        assertEquals("123", resultSet.getString(4));
        assertEquals("Diskmanager", resultSet.getString(5));
        assertEquals("Diskmanager_failed", resultSet.getString(6));
        assertEquals("3", resultSet.getString(7));
        assertEquals(null, resultSet.getString(8));
        assertTrue(resultSet.getString(10).contains("diskManager.dir.fileMask=.*"));
        assertTrue(resultSet.getString(10).contains("failed"));
        assertEquals("Y", resultSet.getString(11));
        assertEquals("2", resultSet.getString(12));
        assertEquals("", resultSet.getString(13));
        assertEquals("", resultSet.getString(14));
        assertEquals("", resultSet.getString(15));
        assertEquals("", resultSet.getString(16));

        assertTrue(resultSet.next());
        assertEquals("((12))", resultSet.getString(1));
        assertEquals("5", resultSet.getString(2));
        assertEquals("1", resultSet.getString(3));
        assertEquals("123", resultSet.getString(4));
        assertEquals("Diskmanager", resultSet.getString(5));
        assertEquals("interface/" + setName, resultSet.getString(6));
        assertEquals("4", resultSet.getString(7));
        assertEquals(null, resultSet.getString(8));
        assertTrue(resultSet.getString(10).contains("diskManager.dir.fileMask=(?\\!.*\\\\.zip$).*"));
        assertTrue(resultSet.getString(10).contains("archive"));
        assertEquals("Y", resultSet.getString(11));
        assertEquals("2", resultSet.getString(12));
        assertEquals("", resultSet.getString(13));
        assertEquals("", resultSet.getString(14));
        assertEquals("", resultSet.getString(15));
        assertEquals("", resultSet.getString(16));

        assertFalse(resultSet.next());

        // Check META_SCHEDULINGS
        resultSet = CreateRockFactoryHelper.executeQueryOnEtlRep("SELECT * FROM META_SCHEDULINGS");
        if (schedulings) {
            assertTrue(resultSet.next());
            assertEquals("((12))", resultSet.getString(1));
            assertEquals("1", resultSet.getString(2));
            assertEquals("weekly", resultSet.getString(3));
            assertEquals(null, resultSet.getString(4));
            assertEquals("1", resultSet.getString(5));
            assertEquals("1", resultSet.getString(6));
            assertEquals("5", resultSet.getString(7));
            assertEquals("0", resultSet.getString(8));
            assertEquals("123", resultSet.getString(9));
            assertEquals("1", resultSet.getString(10));
            assertEquals("Y", resultSet.getString(11));
            assertEquals("Y", resultSet.getString(12));
            assertEquals("Y", resultSet.getString(13));
            assertEquals("Y", resultSet.getString(14));
            assertEquals("Y", resultSet.getString(15));
            assertEquals("Y", resultSet.getString(16));
            assertEquals("Y", resultSet.getString(17));
        } else {
            assertFalse(resultSet.next());
        }
        return true;
    }

}
