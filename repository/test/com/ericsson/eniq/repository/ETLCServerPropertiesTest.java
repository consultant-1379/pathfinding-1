package com.ericsson.eniq.repository;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintWriter;
import java.util.Properties;

import junit.framework.JUnit4TestAdapter;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ETLCServerPropertiesTest {

	static final String VALUE = "etlrep";
	static final String TESTPROP1 = "clear1";
	static final String TESTVAL1 = "test1";
	static final String TESTPROP2 = "clear2";
	static final String TESTVAL2 = "test2";

	static File testUncrypted;
	static File testCrypted;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {

	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

		String tmpFileName = new Long(System.currentTimeMillis()).toString();
		while (new File(tmpFileName).exists()) {
			tmpFileName = new Long(System.currentTimeMillis()).toString();
		}

		String tmpFileName2 = new Long(System.currentTimeMillis()).toString();
		while (new File(tmpFileName2).exists()) {
			tmpFileName2 = new Long(System.currentTimeMillis()).toString();
		}

		testUncrypted = new File(tmpFileName);
		testUncrypted.createNewFile();
		testUncrypted.deleteOnExit();

		PrintWriter pw = new PrintWriter(testUncrypted);
		pw.println(TESTPROP1 + "=" + TESTVAL1);
		pw.println(ETLCServerProperties.DBPASSWORD + "=" + VALUE);
		pw.println(TESTPROP2 + "=" + TESTVAL2);
		pw.close();

		testCrypted = new File(tmpFileName2);
		testCrypted.createNewFile();
		testCrypted.deleteOnExit();

		PrintWriter pw2 = new PrintWriter(testCrypted);
		pw2.println(TESTPROP1 + "=" + TESTVAL1);
		pw2.println(ETLCServerProperties.DBPASSWORD + "=" + AsciiCrypter.getInstance().encrypt(VALUE));
		pw2.println(TESTPROP2 + "=" + TESTVAL2);
		pw2.close();

	}

	@Test
	public void testETLCServerPropertiesCrypted() {
		Properties props = null;
		try {
			props = new ETLCServerProperties(testCrypted.getAbsolutePath());
		} catch (Exception e) {
			fail("ETLCServerProperties cannot given filename");		
		}

		String test = props.getProperty(ETLCServerProperties.DBPASSWORD);

		if (!test.equalsIgnoreCase(VALUE)) {
			fail("ETLCServerProperties cannot handle crypted password");
		}
	}

	@Test
	public void testETLCServerPropertiesUncrypted()  {
		Properties props = null;
		try {
			props = new ETLCServerProperties(testUncrypted.getAbsolutePath());
		} catch (Exception e) {
			fail("ETLCServerProperties cannot given filename");		
		}

		String test = props.getProperty(ETLCServerProperties.DBPASSWORD);

		if (!test.equalsIgnoreCase(VALUE)) {
			fail("ETLCServerProperties cannot handle uncrypted password");
		}
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ETLCServerPropertiesTest.class);
	}
}
