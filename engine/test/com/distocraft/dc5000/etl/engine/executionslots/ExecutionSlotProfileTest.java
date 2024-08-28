package com.distocraft.dc5000.etl.engine.executionslots;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.common.Share;
import com.distocraft.dc5000.etl.engine.main.EngineThread;

public class ExecutionSlotProfileTest {

    // ExecutionSlotProfile
    private static Field name;

    private static Field id;

    private static Field executionSlotList;

    private static Field active;

    // ExecutionSlot
    private static Field runningSet;

    @BeforeClass
    public static void init() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final Class secretClass = esp.getClass();

        final ExecutionSlot es = new ExecutionSlot(0, null);
        final Class secretClass2 = es.getClass();

        try {
            //    ExecutionSlotProfile
            name = secretClass.getDeclaredField("name");
            id = secretClass.getDeclaredField("profileId");
            executionSlotList = secretClass.getDeclaredField("executionSlotList");
            active = secretClass.getDeclaredField("active");

            name.setAccessible(true);
            id.setAccessible(true);
            executionSlotList.setAccessible(true);
            active.setAccessible(true);

            //    ExecutionSlot
            runningSet = secretClass2.getDeclaredField("runningSet");

            runningSet.setAccessible(true);

        } catch (final Exception e) {
            e.printStackTrace();
            fail("init() failed");
        }
    }

    @Test
    public void testActivate() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");

        esp.activate();
        try {
            final Boolean b = (Boolean) active.get(esp);
            assertTrue(b);

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testActivate() failed");
        }
    }

    @Test
    public void testExecutionSlotProfile_max_memory_usage_mbDoesntExist() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        assertNotNull(esp);
    }

    @Test
    public void testExecutionSlotProfile_max_memory_usage_mbSetToSomeValue() {
        final Share share = Share.instance();
        share.add("execution_profile_max_memory_usage_mb", 100);
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        assertNotNull(esp);
    }

    @Test
    public void testAddExecutionSlot() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "name");

        esp.addExecutionSlot(es);

        try {
            final List<ExecutionSlot> v = (List<ExecutionSlot>) executionSlotList.get(esp);
            assertTrue(v.contains(es));

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testAddExecutionSlot() failed");
        }
    }

    @Test
    public void testRemoveExecutionSlotExecutionSlot() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "name");

        final List<ExecutionSlot> v = new ArrayList<ExecutionSlot>();
        v.add(es);

        try {
            executionSlotList.set(esp, v);
            esp.removeExecutionSlot(es);

            final List<ExecutionSlot> rv = (List<ExecutionSlot>) executionSlotList.get(esp);

            assertFalse(rv.contains(es));

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testRemoveExecutionSlotExecutionSlot() failed");
        }
    }

    @Test
    public void testGetRunningSet2() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");

//      Logger l = Logger.getLogger("Log");;
//      EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
      
        final Logger l = Logger.getLogger("Log");;
        final testObject to = new testObject();
        final EngineThread et = new EngineThread("name", "setType1", 10L, to, l);
        et.start();

        try {
            runningSet.set(es2, et);

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testGetRunningSet2() failed");
        }

        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);

        final EngineThread ret = esp.getRunningSet("notExist", 100L);

        assertNull(ret);
    }

    @Test
    public void testGetExecutionSlot() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");

        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);

        final ExecutionSlot res = esp.getExecutionSlot(0);

        assertEquals("ESname1", res.getName());

    }

    @Test
    public void testGetAllExecutionSlots() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");

        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);

        final Iterator it = esp.getAllExecutionSlots();

        final String expected = "ESname1,ESname2";
        String actual = "";
        ExecutionSlot res = (ExecutionSlot) it.next();
        actual += res.getName();
        res = (ExecutionSlot) it.next();
        actual += "," + res.getName();

        assertEquals(expected, actual);

    }

    @Ignore("Test is passing in windows, but failing in Unix")
    @Test
    public void testGetAllFreeExecutionSlots() throws IllegalAccessException {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(1, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(2, "ESname2");
        final ExecutionSlot es3 = new ExecutionSlot(3, "ESname3");

        final Logger l = Logger.getLogger("Log");

        final EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
        //et.start(); // ExecutionSlot es2 is not free anymore

        runningSet.set(es2, et);

        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);
        esp.addExecutionSlot(es3);

        final Iterator it = esp.getAllFreeExecutionSlots();

        final String expected = "ESname1,ESname3";
        String actual = "";
        ExecutionSlot res = (ExecutionSlot) it.next();
        actual += res.getName();
        res = (ExecutionSlot) it.next();
        actual += "," + res.getName();

        assertEquals(expected, actual);
    }

    @Test
//    @Ignore("Test is passing in windows, but failing in Unix")
    public void testGetFirstFreeExecutionSlots() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");
        final ExecutionSlot es3 = new ExecutionSlot(0, "ESname3");

//      Logger l = Logger.getLogger("Log");;
//      EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
//      et.start();
        final Logger l = Logger.getLogger("Log");;
        final testObject to = new testObject();
        final EngineThread et = new EngineThread("type", "setType1", 10L, to, l);
        et.start();

        try {
            runningSet.set(es, et);
            runningSet.set(es2, et);

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testGetFirstFreeExecutionSlots() failed");
        }

        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);
        esp.addExecutionSlot(es3); // only free ExecutionSlot

        final ExecutionSlot res = esp.getFirstFreeExecutionSlots();

        assertEquals("ESname3", res.getName());
    }

    @Test
//    @Ignore("Test is passing in windows, but failing in Unix")
    public void testGetFirstFreeExecutionSlots2() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");

//      Logger l = Logger.getLogger("Log");;
//      EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
//      et.start();
        final Logger l = Logger.getLogger("Log");;
        final testObject to = new testObject();
        final EngineThread et = new EngineThread("type", "setType1", 10L, to, l);

        et.start();

        try {
            runningSet.set(es, et);
            runningSet.set(es2, et);

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testGetFirstFreeExecutionSlots2() failed");
        }

        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);

        final ExecutionSlot res = esp.getFirstFreeExecutionSlots();

        assertNull("null expected", res);
    }

    @Test
    public void testGetNumberOfExecutionSlots() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "name");
        final ExecutionSlot es2 = new ExecutionSlot(0, "name");

        final List<ExecutionSlot> v = new ArrayList<ExecutionSlot>();
        v.add(es);
        v.add(es2);

        try {
            executionSlotList.set(esp, v);

            assertEquals(2, esp.getNumberOfExecutionSlots());

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testGetNumberOfExecutionSlots() failed");
        }
    }

    @Test
//    @Ignore("Test is passing in windows, but failing in Unix")
    public void testGetNumberOfFreeExecutionSlots() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");
        final ExecutionSlot es3 = new ExecutionSlot(0, "ESname3");

//      Logger l = Logger.getLogger("Log");;
//      EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
    
        final Logger l = Logger.getLogger("Log");;
        final testObject to = new testObject();
        final EngineThread et = new EngineThread("type", "setType1", 10L, to, l);
        et.start();

        try {
            runningSet.set(es2, et);

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testGetNumberOfFreeExecutionSlots() failed");
        }

        esp.addExecutionSlot(es); // Free ExecutionSlot
        esp.addExecutionSlot(es2);
        esp.addExecutionSlot(es3); // Free ExecutionSlot

        assertEquals(2, esp.getNumberOfFreeExecutionSlots());
    }

    @Test
//    @Ignore("Test is passing in windows, but failing in Unix")
    public void testGetAllRunningExecutionSlots() throws IllegalArgumentException, IllegalAccessException {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");
        final ExecutionSlot es3 = new ExecutionSlot(0, "ESname3");

        //final Logger l = Logger.getLogger("Log");
        final Logger l = Logger.getLogger("Log");;
        final testObject to = new testObject();

        //final EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
        //et.start();
        final EngineThread et = new EngineThread("type", "setType1", 10L, to, l);
        et.start();
        final EngineThread et2 = new EngineThread("type", "setType2", 10L, to, l);
        et2.start();

        runningSet.set(es2, et);
        runningSet.set(es3, et2);

        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2); // Running ExecutionSlot
        esp.addExecutionSlot(es3); // Running ExecutionSlot

        final Iterator it = esp.getAllRunningExecutionSlots();

        final String expected = "ESname2,ESname3";
        String actual = "";
        ExecutionSlot res = (ExecutionSlot) it.next();
        actual += res.getName();
        res = (ExecutionSlot) it.next();
        actual += "," + res.getName();

        assertEquals(expected, actual);
    }

    @Test
    @Ignore("Test fails randomly on both windows and unix")
    public void testGetAllRunningExecutionSlotSetTypes() throws IllegalArgumentException, IllegalAccessException {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");
        final ExecutionSlot es3 = new ExecutionSlot(0, "ESname3");

        final Logger l = Logger.getLogger("Log");
        ;
        final EngineThread et = new EngineThread("setType1", 10L, l, new EngineCom());
        et.start();
        final EngineThread et2 = new EngineThread("setType2", 10L, l, new EngineCom());
        et2.start();

        runningSet.set(es2, et);
        runningSet.set(es3, et2);







        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2); // Running ExecutionSlot
        esp.addExecutionSlot(es3); // Running ExecutionSlot

        final HashSet hs = (HashSet) esp.getAllRunningExecutionSlotSetTypes();

        assertTrue(hs.contains("setType1"));
        assertTrue(hs.contains("setType2"));
    }

    @Test
    public void testIDAndSetID() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");

        esp.setID("identification");

        assertEquals("identification", esp.ID());
    }

    @Test
    public void testNameAndSetName() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");

        esp.setName("settedName");

        assertEquals("settedName", esp.name());
    }

    @Test
    public void testDeactivate() throws IllegalArgumentException, IllegalAccessException {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");

        active.set(esp, true);
        esp.deactivate();
        final Boolean b = (Boolean) active.get(esp);

        assertFalse(b);






    }

    @Test
    public void testIsActivate() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");

        assertFalse(esp.IsActivate());
    }

    @Test
    public void testNotInExecution2() throws IllegalArgumentException, IllegalAccessException {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");

        final Logger l = Logger.getLogger("Log");

        final EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
        final EngineThread et2 = new EngineThread("name2", 20L, l, new EngineCom());

        runningSet.set(es2, et);







        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);

        assertTrue("True expected", esp.notInExecution(et2));
    }

    @Test
    public void testCheckTable2() throws SecurityException, NoSuchFieldException, IllegalArgumentException,
            IllegalAccessException {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("ESPname", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "ESname1");
        final ExecutionSlot es2 = new ExecutionSlot(0, "ESname2");

        final Logger l = Logger.getLogger("Log");

        final EngineThread et = new EngineThread("name", 10L, l, new EngineCom());
        final EngineThread et2 = new EngineThread("name2", 20L, l, new EngineCom());
        final EngineThread et3 = new EngineThread("name2", 20L, l, new EngineCom());

        final Class secretClass = et.getClass();

        final Field setTables = secretClass.getDeclaredField("setTables");
        setTables.setAccessible(true);

        final ArrayList al = new ArrayList();
        al.add("abc");
        al.add("def");
        final ArrayList al2 = new ArrayList();
        al2.add("foo");
        al2.add("bar");

        setTables.set(et2, al2);
        setTables.set(et3, al);

        runningSet.set(es, et);
        runningSet.set(es2, et2);








        esp.addExecutionSlot(es);
        esp.addExecutionSlot(es2);

        assertFalse("False expected", esp.checkTable(et3));
    }

    @Test
    public void testAreAllSlotsLockedOrFree() throws Exception {

        final List<ExecutionSlot> v = new ArrayList<ExecutionSlot>();

        final ExecutionSlot es = new ExecutionSlot(0, "slot1");
        v.add(es);
        final ExecutionSlot es2 = new ExecutionSlot(0, "slot2");
        v.add(es2);

        final ExecutionSlotProfile esp = new ExecutionSlotProfile("profile", "id");
        executionSlotList.set(esp, v);

        assertTrue("True expected", esp.areAllSlotsLockedOrFree());

    }

    @Test
    @Ignore("Test is passing in windows, but failing in Unix")
    public void testAreAllSlotsLockedOrFree2() throws IllegalArgumentException, IllegalAccessException {

        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "name");
        final ExecutionSlot es2 = new ExecutionSlot(0, "name");

        final List<ExecutionSlot> v = new ArrayList<ExecutionSlot>();
        v.add(es);
        v.add(es2);

        final Logger l = Logger.getLogger("Log");

        final EngineThread et = new EngineThread("setType1", 10L, l, new EngineCom());
        et.start();

        runningSet.set(es2, et);

        executionSlotList.set(esp, v);

        assertEquals(false, esp.areAllSlotsLockedOrFree());
        
    }

    @Test
    public void testCleanProfile() throws IllegalArgumentException, IllegalAccessException {





        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "name");
        es.removeAfterExecution(true);
        final ExecutionSlot es2 = new ExecutionSlot(0, "name");

        final List<ExecutionSlot> v = new ArrayList<ExecutionSlot>();
        v.add(es);
        v.add(es2);

        executionSlotList.set(esp, v);

        esp.cleanProfile();

        final List<ExecutionSlot> rv = (List<ExecutionSlot>) executionSlotList.get(esp);

        assertFalse("False expected", rv.contains(es));
        assertTrue("True expected", rv.contains(es2));






    }

    @Test
    public void testIsProfileClean() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "name");
        es.removeAfterExecution(true);
        final ExecutionSlot es2 = new ExecutionSlot(0, "name");

        final List<ExecutionSlot> v = new ArrayList<ExecutionSlot>();
        v.add(es);
        v.add(es2);

        try {
            executionSlotList.set(esp, v);

            assertFalse("False expected", esp.isProfileClean());

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testIsProfileClean() failed");
        }
    }

    @Test
    public void testIsProfileClean2() {
        final ExecutionSlotProfile esp = new ExecutionSlotProfile("name", "id");
        final ExecutionSlot es = new ExecutionSlot(0, "name");
        final ExecutionSlot es2 = new ExecutionSlot(0, "name");

        final List<ExecutionSlot> v = new ArrayList<ExecutionSlot>();
        v.add(es);
        v.add(es2);

        try {
            executionSlotList.set(esp, v);

            assertTrue("True expected", esp.isProfileClean());

        } catch (final Exception e) {
            e.printStackTrace();
            fail("testIsProfileClean2() failed");
        }
    }

    private class testObject implements Runnable {

        public void run() {
            System.out.println("testObject started");

        }

    }

}
