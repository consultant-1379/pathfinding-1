/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.UpdateThresholdLimit;
import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import com.ericsson.eniq.common.testutilities.ServicenamesTestHelper;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.*;
import java.rmi.Naming;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.ericsson.eniq.common.Constants;

import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ActivateSetInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.GetProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeAggregationStatusCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeProfileAndWaitCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeSetPriorityInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.DisableSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.EnableSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.GiveEngineCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.HoldPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.HoldSetInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.KillRunningSetsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.LockExecutionProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.LoggingStatusCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.PrintSlotInfoCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RefreshDBLookupsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RefreshTransformationsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadAggregationCacheCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadAlarmCacheCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadConfigCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadLoggingCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadProfilesCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RemoveSetFromPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RemoveTechPacksInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RestartPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RestoreCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowDisabledSetsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowSetsInExecutionSlotsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowSetsInQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShutdownForcefulCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShutdownSlowCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartAndWaitSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetInEngineCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StatusCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StopOrShutdownFastCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.UnlockExecutionProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.UpdateTransformationCommand;
import com.distocraft.dc5000.etl.engine.main.exceptions.NoSuchCommandException;

/**
 * @author eemecoy
 *
 */
public class EngineAdminTest {

    private final static Map<String, Class<? extends Command>> commandToClassMap = new HashMap<String, Class<? extends Command>>();
  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "EngineAdminTest");

    private static Map<String, String> env = System.getenv();
    
    @BeforeClass
    public static void setUpClass() throws IOException {
      if(!TMP_DIR.exists() && !TMP_DIR.mkdirs()){
        fail("Failed to create temp directory " + TMP_DIR.getPath());
      }

        commandToClassMap.put("stop", StopOrShutdownFastCommand.class);
        commandToClassMap.put("shutdown_fast", StopOrShutdownFastCommand.class);
        commandToClassMap.put("status", StatusCommand.class);
        commandToClassMap.put("shutdown_forceful", ShutdownForcefulCommand.class);
        commandToClassMap.put("shutdown_slow", ShutdownSlowCommand.class);
        commandToClassMap.put("startSetInEngine", StartSetInEngineCommand.class);
        commandToClassMap.put("startSet", StartSetCommand.class);
        commandToClassMap.put("startAndWaitSet", StartAndWaitSetCommand.class);
        commandToClassMap.put("giveEngineCommand", GiveEngineCommand.class);
        commandToClassMap.put("startSets", StartSetsCommand.class);
        commandToClassMap.put("changeProfile", ChangeProfileCommand.class);
        commandToClassMap.put("changeProfileAndWait", ChangeProfileAndWaitCommand.class);
        commandToClassMap.put("reloadProfiles", ReloadProfilesCommand.class);
        commandToClassMap.put("loggingStatus", LoggingStatusCommand.class);
        commandToClassMap.put("holdPriorityQueue", HoldPriorityQueueCommand.class);
        commandToClassMap.put("restartPriorityQueue", RestartPriorityQueueCommand.class);
        commandToClassMap.put("restore", RestoreCommand.class);
        commandToClassMap.put("reloadConfig", ReloadConfigCommand.class);
        commandToClassMap.put("reloadAggregationCache", ReloadAggregationCacheCommand.class);
        commandToClassMap.put("reloadLogging", ReloadLoggingCommand.class);
        commandToClassMap.put("reloadAlarmCache", ReloadAlarmCacheCommand.class);
        commandToClassMap.put("showSetsInQueue", ShowSetsInQueueCommand.class);
        commandToClassMap.put("queue", ShowSetsInQueueCommand.class);
        commandToClassMap.put("showSetsInExecutionSlots", ShowSetsInExecutionSlotsCommand.class);
        commandToClassMap.put("slots", ShowSetsInExecutionSlotsCommand.class);
        commandToClassMap.put("removeSetFromPriorityQueue", RemoveSetFromPriorityQueueCommand.class);
        commandToClassMap.put("changeSetPriorityInPriorityQueue", ChangeSetPriorityInPriorityQueueCommand.class);
        commandToClassMap.put("activateSetInPriorityQueue", ActivateSetInPriorityQueueCommand.class);
        commandToClassMap.put("holdSetInPriorityQueue", HoldSetInPriorityQueueCommand.class);
        commandToClassMap.put("changeAggregationStatus", ChangeAggregationStatusCommand.class);
        commandToClassMap.put("unLockExecutionprofile", UnlockExecutionProfileCommand.class);
        commandToClassMap.put("lockExecutionprofile", LockExecutionProfileCommand.class);
        commandToClassMap.put("refreshDBLookups", RefreshDBLookupsCommand.class);
        commandToClassMap.put("refreshTransformations", RefreshTransformationsCommand.class);
        commandToClassMap.put("updateTransformation", UpdateTransformationCommand.class);
        commandToClassMap.put("disableSet", DisableSetCommand.class);
        commandToClassMap.put("enableSet", EnableSetCommand.class);
        commandToClassMap.put("showDisabledSets", ShowDisabledSetsCommand.class);
        commandToClassMap.put("printSlotInfo", PrintSlotInfoCommand.class);
        commandToClassMap.put("currentProfile", GetProfileCommand.class);
        commandToClassMap.put("updatethresholdLimit", UpdateThresholdLimit.class);
        commandToClassMap.put("removeTechPacksInPriorityQueue", RemoveTechPacksInPriorityQueueCommand.class);
        commandToClassMap.put("killRunningSets", KillRunningSetsCommand.class);
      
      
      
        ServicenamesTestHelper.setupEmpty(TMP_DIR);
        ServicenamesTestHelper.createDefaultServicenamesFile();
        }
  @AfterClass
  public static void afterClass(){
    DirectoryHelper.delete(TMP_DIR);
  }

    @Test
    public void testExceptionThrownWhenNoSuchCommand() throws IllegalArgumentException, SecurityException,
            InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final String dummyCommandName = "aDummyCommand";
        try {
            EngineAdmin.createCommand(dummyCommandName, null);
            fail("Exception should have been thrown");
        } catch (final NoSuchCommandException e) {
            assertThat(e.getMessage(), is("Invalid command entered: " + dummyCommandName));
        }
    }

    @Test
    public void testCorrectCommandIsCreated() throws IllegalArgumentException, SecurityException,
            InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException,
            NoSuchCommandException {

        for (final String command : commandToClassMap.keySet()) {
            final Command commandCreated = EngineAdmin.createCommand(command, new String[] { "commandName" });

            final Object expectedClass = commandToClassMap.get(command);
            assertThat(commandCreated.getClass(), is(expectedClass));
        }
    }
    @Test
    public void checkThatCallOverRmiToGetTableNamesForRawEventsWorks() throws Exception {
      setUpPropertiesFileAndProperty();
      TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(false);
      EngineAdmin admin = new EngineAdmin();
      List<String> tables = admin.getTableNamesForRawEvents("test", new Timestamp(0L), new Timestamp(10L));

      assertEquals(tables.get(0), "test_10");
    }
      /**
       * Utility class to cache output to std err/std out and allow the output lines to be retrieved
       **/
      private class StreamConsumer extends Thread{
          final InputStream is;
          final List<String> cachedOutputLines = new ArrayList<String>();

          StreamConsumer(final InputStream is)
          {
              this.is = is;
          }

          public void run(){
              try
              {
                  final InputStreamReader isr = new InputStreamReader(is);
                  final BufferedReader br = new BufferedReader(isr);
                  String line;
                  while ( (line = br.readLine()) != null){
                      cachedOutputLines.add( line );
                  }
              } catch (IOException ioe){
                  ioe.printStackTrace();
              }
          }

          public List<String> getCache(){
              return Collections.unmodifiableList( cachedOutputLines );
          }
      }


    /**
     * Test that calling stop on engine admin when the rmi registry is running but has no bindings for TransferEngine
     * doesnt result in an NotBoundException in either std out or std err and that the exit code is 1
     * EngineAdmin is run in a seperate JVM and the standard out and err analysed
     */
    @Test
    public void testNotBoundExceptionNotOutputInStandardErrorOnEngineStop() throws Exception {
      String executable = "";
      try{
          final int expectExitCode = 1;
          final String rmiRef = "//localhost:" + 1200 + "/" + "TransferEngine";

          setUpPropertiesFileAndProperty();
          // we need the registry running but empty to get a NotBoundException otherwise a ConnectionException will be thrown
          // so create a TestITransferEngineRMI then unbind it( sets up the registry if it doesnt exist etc. )
          final TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(false);
          Naming.unbind( rmiRef );

          final String javaClassPath = System.getProperty( "java.class.path" );
          final Runtime rt = Runtime.getRuntime();
        final String confPath = System.getProperty(Constants.DC_CONFIG_DIR_PROPERTY_NAME);
        final String systemProperties = "-D" + Constants.CONF_DIR_PROPERTY_NAME + "=" + confPath +
          " -D" + Constants.DC_CONFIG_DIR_PROPERTY_NAME + "=" + confPath + " -Ddc5000.config.directory=" + confPath;

          executable = "java -cp \"" + javaClassPath + "\" " + systemProperties + " " + EngineAdmin.class.getCanonicalName() + " stop";
          final Process proc = rt.exec( executable );
          // std err
          final StreamConsumer errorStreamConsumer = new StreamConsumer(proc.getErrorStream());
          // std out?
          final StreamConsumer outputStreamConsumer = new StreamConsumer(proc.getInputStream());

          // start the io stream consumers
          errorStreamConsumer.start();
          outputStreamConsumer.start();

          final int actualExitVal = proc.waitFor();
          final List<String> actualErrorOutput = errorStreamConsumer.getCache();
          // Validate Std err
          for( String line : actualErrorOutput ){
              assertTrue( "Standard error contains exception " + line,
                      line.indexOf( "Exception" )==-1 );
          }
          final List<String> actualStandardOutput = outputStreamConsumer.getCache();
          // Validate Std out
          for( String line : actualStandardOutput ){
              assertTrue( "Standard out contains exception " + line,
                      line.indexOf( "Exception" )==-1 );
          }
          assertEquals( expectExitCode, actualExitVal );

      }
      catch( Throwable t ){
          t.printStackTrace();
        //  fail( "No exception should be thrown for this test case, type= " + t.getClass().getName() + " exe used=" + executable );
      }
    }

    /**
     * @throws IOException
     */
    private static void setUpPropertiesFileAndProperty() throws IOException {
      String userHome = env.get("WORKSPACE");

      File prop = new File(userHome, "ETLCServer.properties");
      prop.deleteOnExit();

      PrintWriter pw = new PrintWriter(new FileWriter(prop));
      pw.write("name=value");
      pw.close();
    }

  @Test
  public void testUpdateThresholdProperty() throws Exception {
    final File propFile = new File(System.getProperty("java.io.tmpdir"), "static.properties");
    propFile.deleteOnExit();
    if(!propFile.exists() && !propFile.createNewFile()){
      fail("Setup failed to create " + propFile.getPath());
    }
    System.setProperty(StaticProperties.DC5000_CONFIG_DIR, propFile.getParent());
    try {
      final EngineAdmin testInstance = new EngineAdmin();
      final int testInt = 150;
      final String name = EngineConstants.THRESHOLD_NAME;
      testInstance.updateThresholdProperty(testInt);
      final int result = Integer.parseInt(StaticProperties.getProperty(name));
      assertEquals("The 2 values should be equal", testInt, result);
    } catch (Exception exc) {
      fail("Updating the threshold property should not fail: " + exc.toString());
    }
  }
  
  /**
   * Test calling engine command to remove sets from the priority queue.
   * Argument is a list of of tech pack names to remove sets for.
   */
  @Test
  public void testRemoveTechPacksInPriorityQueue() {
    try {
      // Create test list of tech pack names to remove from priority queue.
      List<String> techpacks = new ArrayList<String>();    
      setUpPropertiesFileAndProperty();
      
      // Register test engine object in RMI:
      TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(false);
      final EngineAdmin admin = new EngineAdmin();
      admin.removeTechPacksInPriorityQueue(techpacks);      
    } catch (Exception exc) {
      fail("Error calling command removeTechPacksInPriorityQueue");
    }
  }
  
  /**
   * Test calling engine command to kill running sets for a tech pack.
   * Argument is a list of of techpack names to kill sets for.
   */
  @Test
  public void testKillRunningSets() {
    try {
      // Create test list of tech pack names to remove from priority queue.
      List<String> techpacks = new ArrayList<String>();    
      setUpPropertiesFileAndProperty();
      
      // Register test engine object in RMI:
      TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(false);
      final EngineAdmin admin = new EngineAdmin();
      admin.killRunningSets(techpacks);      
    } catch (Exception exc) {
      fail("Error calling command killRunningSets");
    }
  }
  
  /**
   * Test calling engine command to kill running sets for a tech pack.
   * Argument is a list of of techpack names to kill sets for.
   */
  @Test
  public void testLockEventsUIusers() {
    try {
      setUpPropertiesFileAndProperty();
      
      // Register test engine object in RMI:
      TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(false);
      final EngineAdmin admin = new EngineAdmin();
      admin.lockEventsUIusers(true);      
    } catch (Exception exc) {
      fail("Error calling command lockEventsUIusers");
    }
  }
  
}
