package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.After;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.common.StaticProperties;

/**
 * 
 * @author ejarsok
 * 
 */

public class DistributeTest {

  private static Distribute dist;

  private static Class secretClass;

  private static File outDir;

  private static File inDir;
  
  private static String outDir2;

  private static String inDir2;

  private static File file1;

  private static File file2;
  
  private static Statement stm;
  
  private static Map<String, String> env = System.getenv();


  @Before
	public void init(){
		init(true, env.get("WORKSPACE"), "Distribute_MOM_EBSG");
	}
  public void init(final boolean createDirs, final String baseDir, final String setName) {
    outDir = new File(baseDir, "outDir");
    inDir = new File(baseDir, "inDir");
		if(createDirs){
    outDir.mkdir();
    inDir.mkdir();
		}

    //
    String[] array = inDir.getAbsolutePath().split("\\\\");
    inDir2 = "";
		for (String anArray : array) {
			inDir2 += anArray + "/";
		}

    array = outDir.getAbsolutePath().split("\\\\");
    outDir2 = "";
		for (String anArray : array) {
			outDir2 += anArray + "/";
		}

    file1 = new File(inDir, "File1");
    file2 = new File(inDir, "File2");

		if(createDirs){
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file1));
      pw.print("foobar");
      pw.close();
      pw = new PrintWriter(new FileWriter(file2));
      pw.print("foobar");
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Can´t write in file!");
    }
		}

    Long collectionSetId = 1L;
    Long transferActionId = 1L;
    Long transferBatchId = 1L;
    Long connectId = 1L;
    RockFactory rockFact = null;

    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e2) {
      e2.printStackTrace();
      fail("init() failed, ClassNotFoundException");
    }

    Connection c;
    try {
      c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
      stm = c.createStatement();

      stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
          + "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

      stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', '"+setName+"', 'description', '1', 'Y', 'type')");

    } catch (SQLException e1) {
      e1.printStackTrace();
      fail("init() failed, SQLException");
    }

    try {
      rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
    } catch (SQLException e) {
      e.printStackTrace();
      fail("init() failed, SQLException");
    } catch (RockException e) {
      e.printStackTrace();
      fail("init() failed, RockException");
    }
    Meta_versions version = new Meta_versions(rockFact);
    Meta_collections collection = new Meta_collections(rockFact);
    Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
		trActions.setTransfer_action_name(setName);

    file1.setLastModified(System.currentTimeMillis() - 700000);
    file2.setLastModified(System.currentTimeMillis() - 700000);
    trActions.setAction_contents("method=move\ninDir=" + inDir2 + "\ndefaultOutDir=" + outDir2
        + "\nminFileAge=1\npattern=f*r\ntype=content\nbufferSize=6\noutDir=" + outDir2);

    try {
      dist = new Distribute(version, collectionSetId, collection, transferActionId, transferBatchId, connectId,
          rockFact, trActions);
      secretClass = dist.getClass();
    } catch (EngineMetaDataException e) {
      e.printStackTrace();
      fail("init() failed, EngineMetaDataException");
    }
  }
  
  @Test
  public void testCopyFile() {
    try {
      File file4 = new File(inDir, "File4");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file4));
        pw.print("foobar");
        pw.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Can´t write in file!");
      }
      
      Method method = secretClass.getDeclaredMethod("copyFile", new Class[] {File.class, String.class});
      method.setAccessible(true);
      method.invoke(dist, new Object[] {file4, outDir2});
      File x = new File(outDir2, "File4");
      
      // file is copyed
      assertEquals(true, x.exists());
      x.delete();
      file4.delete();
    } catch (Exception e) {
      e.printStackTrace();
      fail("testCopyFile() failed, Exception");
    }
  }
  
  @Test
  public void testMoveFile() {
    try {
      File file5 = new File(inDir, "File5");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file5));
        pw.print("foobar");
        pw.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Can´t write in file!");
      }
      
      Method method = secretClass.getDeclaredMethod("moveFile", new Class[] {File.class, String.class});
      method.setAccessible(true);
      method.invoke(dist, new Object[] {file5, outDir2});
      
      File x = new File(inDir2, "File5");
      
      // original file is deleted
      assertEquals(false, x.exists());
      x = new File(outDir2, "File5");
      
      // file is copyed/moved
      assertEquals(true, x.exists());
      x.delete();
      file5.delete();
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testMoveFile() failed, Exception");
    }
  }
  
  @Test
  public void testDistributeFile() {
    // TODO assertion fix
    try {
      File file6 = new File(inDir, "File6");
      File file7 = new File(inDir, "File7");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file6));
        pw.print("foobar");
        pw.close();
        pw = new PrintWriter(new FileWriter(file7));
        pw.print("foobar");
        pw.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Can´t write in file!");
      }
      
      Field field = secretClass.getDeclaredField("method");
      Method method = secretClass.getDeclaredMethod("distributeFile", new Class[] {File.class, String.class});
      field.setAccessible(true);
      method.setAccessible(true);
      field.set(dist, "copy");
      method.invoke(dist, new Object[] {file6, outDir2});
      File x = new File(inDir2, "File6");
      
      // File6 still exist in inDir2
      assertEquals(true, x.exists());
      x = new File(outDir2, "File6");
      
      // File6 is copyed into outDir2
      assertEquals(true, x.exists());
      x.delete();
      
      field.set(dist, "move");
      method.invoke(dist, new Object[] {file7, outDir2});
      x = new File(inDir2, "File7");
      
      // File7 is removed from inDir2
      assertEquals(false, x.exists());
      x = new File(outDir2, "File7");
      
      // File7 is moved into inDir2
      assertEquals(true, x.exists());
      x.delete();   
      file6.delete();
      file7.delete();
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testDistributeFile() failed, Exception");
    }
  }
  
  @Test
  public void testCreateFileList() {
    try {
      Field field = secretClass.getDeclaredField("inDir");
      Method method = secretClass.getDeclaredMethod("createFileList", null);
      field.setAccessible(true);
      method.setAccessible(true);
      field.set(dist, inDir.getAbsolutePath());
      List vec = (List) method.invoke(dist, null);

			for (Object aVec : vec) {
				File x = (File) aVec;
        int i = Integer.parseInt(x.getAbsolutePath().substring(x.getAbsolutePath().length() - 1,
            x.getAbsolutePath().length()));
        switch (i) {
        case 1:
          assertEquals(file1.getAbsolutePath(), x.getAbsolutePath());
          break;
        case 2:
          assertEquals(file2.getAbsolutePath(), x.getAbsolutePath());
          break;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("testCreateFileList() failed, Exception");
    }
  }

  @Test
  public void testCreateFileListNonExistantInDirActionDistribute_MOM_EBSG() throws Exception {
	    Properties sprop = new Properties();
	    sprop.setProperty("distributor.ignore_error_sets", "Distribute_MOM_EBSG;Distribute_MOM_EBSS;Distribute_MOM_EBSW");
	    
	    StaticProperties.giveProperties(sprop);
	    
    try {
      Field field = secretClass.getDeclaredField("inDir");
      Method method = secretClass.getDeclaredMethod("createFileList", null);
      field.setAccessible(true);
      method.setAccessible(true);
      List vec = (List) method.invoke(dist, null);

			for (Object aVec : vec) {
				File x = (File) aVec;
				int i = Integer.parseInt(x.getAbsolutePath().substring(x.getAbsolutePath().length() - 1,
					 x.getAbsolutePath().length()));
				switch (i) {
					case 1:
						assertEquals(file1.getAbsolutePath(), x.getAbsolutePath());
						break;
					case 2:
						assertEquals(file2.getAbsolutePath(), x.getAbsolutePath());
						break;
				}
			}

    } catch (Exception e) {
      fail("testCreateFileList() failed, Exception");
    }
  }

  @Test
  public void testIsOldEnough() {
    try {
      File file3 = new File(inDir, "File3");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file3));
        pw.print("foobar");
        pw.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Can´t write in file!");
      }
      
      Method method = secretClass.getDeclaredMethod("isOldEnough", new Class[] {File.class});
      method.setAccessible(true);
      file3.setLastModified(System.currentTimeMillis() - 100000);
      assertEquals(true, method.invoke(dist, new Object[] {file3}));
      file3.setLastModified(System.currentTimeMillis() + 10000);
      assertEquals(false, method.invoke(dist, new Object[] {file3}));
      
      file3.delete();
    } catch (Exception e) {
      e.printStackTrace();
      fail("testIsOldEnough() failed, Exception");
    }
  }
  
  @Test
  public void testGetBuffer() {
    file1 = new File(inDir, "File1");
    
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file1));
      pw.print("foobar");
      pw.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Can´t write in file!");
    }
    
    try {
      Field field = secretClass.getDeclaredField("type");
      Field field2 = secretClass.getDeclaredField("bufferSize");
      Method method = secretClass.getDeclaredMethod("getBuffer", new Class[] {File.class});
      field.setAccessible(true);
      field2.setAccessible(true);
      method.setAccessible(true);
      field.set(dist, "filename");
      System.out.println("testGetBuffer()" + file1.getName() + " " + file1.isFile() + " " + file1.canRead()  + " " +  file1.exists()  + " " +  file1.length());
      String buf = (String) method.invoke(dist, new Object[] {file1});
      assertEquals("File1", buf);
      
      field.set(dist, "content");
      field2.set(dist, 6);
      buf = (String) method.invoke(dist, new Object[] {file1});
      assertEquals("foobar", buf);
   
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetBuffer() failed, Exception");
    }
  }
	private void setStaticProperties(final Properties props) {
//		final String confDir = System.getProperty("java.io.tmpdir");
//		System.setProperty("CONF_DIR", confDir);
//		final String fName = confDir + "/ETLCServer.properties";
//		try {
//			final FileOutputStream fos = new FileOutputStream(fName);
//			props.store(fos, "");
//			fos.close();
//		} catch (IOException e) {
//			fail("Testcase setup failed : " + e);
//		}
		try {
			StaticProperties.giveProperties(props);
		} catch (Exception e) {
			// doesnt actually throw anything...
		}
	}
	private void runDistributeTestNoInDir(final String actionName, final boolean ignoreError){
		try{
			clean();
			final Properties p = new Properties();
			if(ignoreError){
				p.setProperty(Distribute.DISTRIB_IGNORE, "abc;"+actionName+";def");
			}
			setStaticProperties(p);
			init(false, "C:/does/not/compute", actionName);
			dist.execute();
			if(!ignoreError){
				fail("Error was ignored, it shouldn't have been..");
			}
		} catch (EngineException e){
			if(ignoreError){
				fail("inDir error should have been ingnored, it wasnt : " + e.getNestedException());
			} else {
				if(e.getNestedException() != null){
					assertEquals("inDir doesn't exist or cannot be read.", e.getNestedException().getMessage());
				} else {
					e.getNestedException().printStackTrace();
					fail("runDistributeTestNoInDir() failed, Exception");
				}
			}
		} catch (Exception e){
			// StaticProperties.giveProperties() throws this, change the method....
			e.printStackTrace();
			fail("runDistributeTestNoInDir() failed, Exception");
		}
	}

	@Test
	public void testShouldError(){
		final Properties p = new Properties();
		final String aName = "12345";
		p.setProperty(Distribute.DISTRIB_IGNORE, "abc;"+aName+";def");
		setStaticProperties(p);
		boolean shouldError = dist.errorNonExistingInDir(aName);
		assertFalse("Should Error be false as the property is set", shouldError);
		shouldError = dist.errorNonExistingInDir("something_else");
		assertTrue("Should Error as the action name is not in the property", shouldError);
		p.clear();
		setStaticProperties(p);
		shouldError = dist.errorNonExistingInDir("something_else");
		assertTrue("Should error be true as there's no property set ", shouldError);
	}

	@Test
	public void testExecuteCheckErrors(){
		runDistributeTestNoInDir("Distributor_MOM_EBSW", false);
	}


	@Test
	public void testExecuteIgnoreErrors(){
		runDistributeTestNoInDir("Distributor_MOM_EBSW", true);
	}
  
  @Ignore
  public void testExecute() {
    try {
      dist.execute();
      File x = new File(outDir, "File1"+System.currentTimeMillis());
      assertEquals(true, x.exists());
      x = new File(outDir, "File2"+System.currentTimeMillis());
      assertEquals(true, x.exists());
      x.delete();
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testExecute() failed, Exception");
    }
  }
  
  @After
  public void clean() {
    File filelist[] = outDir.listFiles();
		if(filelist != null){
			for (File x : filelist) {
      x.delete();
			}
    }
    filelist = inDir.listFiles();
		if(filelist != null){
			for (File x : filelist) {
      x.delete();
    }
		}
    try {
      stm.executeUpdate("SHUTDOWN");
    } catch (SQLException e) {
      e.printStackTrace();
    }
    outDir.delete();
    inDir.delete();
  }
}
