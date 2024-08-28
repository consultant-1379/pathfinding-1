/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2013 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * Test for LockEventsUIusers command.
 * 
 * @author eciacah
 */
public class LockEventsUIusersTest extends BaseMock {

  EngineAdmin mockedEngineAdmin;

  @Before
  public void setUp() {
    mockedEngineAdmin = context.mock(EngineAdmin.class);
  }

  @Test
  public void testCheckAndConvertArgumentTypes() throws InvalidArgumentsException {
    String onOrOff = "off";
    String[] arguments = new String[] { "LockEventsUIusers", onOrOff };
    LockEventsUIusersCommand command = new LockEventsUIusersCommand(arguments);    
    command.checkAndConvertArgumentTypes();
    assertTrue(command.arguments[0] == "LockEventsUIusers");
    assertTrue(command.arguments[1] == "off");
    assertTrue("Lock flag should be false if 'off' command used", !command.isLocked());
    
    onOrOff = "on";
    arguments = new String[] { "LockEventsUIusers", onOrOff };
    command = new LockEventsUIusersCommand(arguments);    
    command.checkAndConvertArgumentTypes();
    assertTrue(command.arguments[0] == "LockEventsUIusers");
    assertTrue(command.arguments[1] == "on");
    assertTrue("Lock flag should be true if 'on' command used", command.isLocked());        
  }
  
  /**
   * checkAndConvertArgumentTypes() should throw an exception if an invalid
   * @throws InvalidArgumentsException
   */
  @Test(expected = InvalidArgumentsException.class) 
  public void testCheckAndConvertArgumentTypesException() throws InvalidArgumentsException {
    String onOrOff = "invalidString";
    String[] arguments = new String[] { "LockEventsUIusers", onOrOff };
    LockEventsUIusersCommand command = new LockEventsUIusersCommand(arguments);    
    command.checkAndConvertArgumentTypes();
  }
  
  @Test
  public void testGetUsageMessage() {
    final String onOrOff = "off";
    final String[] arguments = new String[] { "LockEventsUIusers", onOrOff };
    final Command command = new StubbedLockEventsUIusersCommand(arguments);
    assertNotNull(command.getUsageMessage());
  }

  @Test
  public void testPerformCommandLockUsers() throws Exception {
    final String onOrOff = "on";
    final String[] arguments = new String[] { "LockEventsUIusers", onOrOff };
    
    final Command command = new StubbedLockEventsUIusersCommand(arguments) {
      protected String getServerType() {
        return "events";
      }
    };

    context.checking(new Expectations() {
      {
        one(mockedEngineAdmin).lockEventsUIusers(true);
      }
    });
    command.validateArguments();
    command.performCommand();
  }
  
  @Test
  public void testPerformCommandUnlockUsers() throws Exception {
    final String onOrOff = "off";
    final String[] arguments = new String[] { "LockEventsUIusers", onOrOff };
    
    final Command command = new StubbedLockEventsUIusersCommand(arguments) {
      protected String getServerType() {
        return "events";
      }
    };

    context.checking(new Expectations() {
      {
        one(mockedEngineAdmin).lockEventsUIusers(false);
      }
    });
    command.validateArguments();
    command.performCommand();
  }
  
  /**
   * Test locking users on Stats.
   * EngineAdmin.lockEventsUIusers() should not be called. 
   * @throws Exception
   */
  @Test
  public void testPerformCommandOnStats() throws Exception {
    final String onOrOff = "on";
    final String[] arguments = new String[] { "LockEventsUIusers", onOrOff };
    
    final Command command = new StubbedLockEventsUIusersCommand(arguments) {
      protected String getServerType() {
        return "stats";
      }
    };

    command.validateArguments();
    command.performCommand();
  }

  /**
   * Number of arguments should be 2.
   * LockEventsUIusers on | off.
   */
  @Test
  public void testGetCorrectArgumentsLength() {
    final String onOrOff = "off";
    final String[] arguments = new String[] { "LockEventsUIusers", onOrOff };
    
    final Command command = new StubbedLockEventsUIusersCommand(arguments);
    assertTrue("Number of arguments for LockEventsUIusers should be 2", 
        command.getCorrectArgumentsLength() == 2);  
  }

  
  class StubbedLockEventsUIusersCommand extends LockEventsUIusersCommand {

    /**
     * @param args
     */
    public StubbedLockEventsUIusersCommand(final String[] args) {
      super(args);
    }

    @Override
    protected EngineAdmin createNewEngineAdmin() {
      return mockedEngineAdmin;
    }
  }

}
