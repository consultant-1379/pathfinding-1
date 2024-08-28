package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;

/**
 * @author ejarsok
 * 
 */
public class SetContextTriggerActionTest {

	private OverwrittenSetContextTriggerAction sc = new OverwrittenSetContextTriggerAction();


  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "SetContextTriggerActionTest");
	
	@BeforeClass
	public static void init() {
    DirectoryHelper.mkdirs(TMP_DIR);
	    System.setProperty("dc5000.config.directory", TMP_DIR.getPath());
	    File prop = new File(TMP_DIR, "ETLCServer.properties");
		prop.deleteOnExit();
		try {
			PrintWriter pw = new PrintWriter(new FileWriter(prop));
			pw.write("name=value");
			pw.close();
		} catch (IOException e3) {
			e3.printStackTrace();
			fail("Can't write in file");
		}
	}

  @AfterClass
  public static void afterClass(){
    DirectoryHelper.delete(TMP_DIR);
  }

	@Test
	public void testIsEqual() {
		// TODO assertion fix
		try {
			assertEquals(false, sc.isEqual(null, "foobar"));
			assertEquals(true, sc.isEqual(true, "true"));
			assertEquals(false, sc.isEqual(true, "false"));
			assertEquals(false, sc.isEqual("notBoolean", "foobar"));
			assertEquals(true, sc.isEqual(1, "1"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIsMore() {
		// TODO assertion fix
		try {
			assertEquals(true, sc.isMore(1, "2"));
			assertEquals(false, sc.isMore(2, "1"));
			assertEquals(false, sc.isMore(null, "2")); // false if null
			assertEquals(false, sc.isMore(1, null)); // false if null
		} catch (Exception e) {
			e.printStackTrace();
			fail("testIsMore() failed");
		}
	}

	@Test
	public void testIsLess() {
		// TODO assertion fix
		try {
			assertEquals(true, sc.isLess(2, "1"));
			assertEquals(false, sc.isLess(1, "2"));
			assertEquals(false, sc.isLess(null, "2")); // false if null
			assertEquals(false, sc.isLess(1, null)); // false if null
		} catch (Exception e) {
			e.printStackTrace();
			fail("testIsMore() failed");
		}
	}

	@Test
	public void testTrigger() throws Exception {
		final TestScheduler ts = (TestScheduler)sc.connect();
		ts.clear();
		
		final List<String> l = new ArrayList<String>();
		l.add("tName1");
		l.add("tName2");
		sc.trigger(l, ":");

		assertEquals(true, ts.isTriggered(":tName1"));
		assertEquals(true, ts.isTriggered(":tName2"));
		assertEquals(new Integer(2), ts.size());

	}

	@Test
	public void testContains() {
		// TODO assertion fix
		HashSet s = new HashSet();
		s.add("foo");
		s.add("bar");

		try {
			assertEquals(true, sc.contains(s, "foo,bar"));
			assertEquals(false, sc.contains(s, "not,in,set"));
			assertEquals(false, sc.contains(null, "foo,bar"));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testStrToList() {
		ArrayList al = new ArrayList();
		ArrayList al2 = new ArrayList();
		al2.add("foo");
		al2.add("bar");

		al = (ArrayList) sc.strToList("foo,bar");
		assertEquals(true, al.containsAll(al2));
	}

	@Test
	public void testSetToList() {
		HashSet s = new HashSet();
		ArrayList al = new ArrayList();
		s.add("foo");
		s.add("bar");

		al = (ArrayList) sc.setToList(s);
		assertEquals(true, s.containsAll(al));
	}

	public class OverwrittenSetContextTriggerAction extends SetContextTriggerAction {
		
		private final TestScheduler scheduler = new TestScheduler();
		
		protected ISchedulerRMI connect() {
			return scheduler;
		}
				
	};
		
}
