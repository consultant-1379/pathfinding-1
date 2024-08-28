/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2013 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.junit.Assert.*;
import java.util.List;

import junit.framework.Assert;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * Engine command to remove sets in the priority queue for a list of tech packs.
 * 
 * @author eciacah
 */
public class RemoveTechPacksInPriorityQueueCommandTest extends BaseMock {

  EngineAdmin mockedEngineAdmin;

  @Before
  public void setUp() {
    mockedEngineAdmin = context.mock(EngineAdmin.class);
  }

  /**
   * Basic call to removeTechPacksInPriorityQueue().
   * 
   * @throws NumberFormatException
   * @throws Exception
   */
  @Test
  public void testPerformCommand() throws NumberFormatException, Exception {

    // Set up list of tech packs to kill:
    final String[] args = new String[4];
    args[0] = "removeTechPackInPriorityQueue";
    args[1] = "DC_E_MGW";
    args[2] = "DC_E_CPP";
    args[3] = "DC_E_SGSN";

    RemoveTechPacksInPriorityQueueCommand test = new RemoveTechPacksInPriorityQueueCommand(args) {

      @Override
      protected EngineAdmin createNewEngineAdmin() {
        return mockedEngineAdmin;
      }
    };

    context.checking(new Expectations() {

      {
        one(mockedEngineAdmin).removeTechPacksInPriorityQueue(with(any((List.class))));
      }
    });

    try {
      test.performCommand();
    } catch (Exception exc) {
      fail("Call to removeTechPacksInPriorityQueue() failed");
    }
  }

  /**
   * engine -e removeTechPackInPriorityQueue DC_E_MGW should be allowed by
   * checkNumberOfArguments().
   */
  @Test
  public void testCheckNumberOfArguments_oneTP() {
    /**
     * Set up array of arguments. For the command: engine -e
     * removeTechPackInPriorityQueue DC_E_MGW Only removeTechPackInPriorityQueue
     * DC_E_MGW will be passed in as arguments. This should be allowed by
     * checkNumberOfArguments().
     */
    final String[] args = new String[2];
    args[0] = "removeTechPackInPriorityQueue";
    args[1] = "DC_E_MGW";

    // Set up test instance. Override createNewEngineAdmin() to return the mock
    // object:
    RemoveTechPacksInPriorityQueueCommand test = new RemoveTechPacksInPriorityQueueCommand(args);

    try {
      test.checkNumberOfArguments();
    } catch (InvalidArgumentsException e) {
      fail("checkNumberOfArguments() should allow 2 or more arguments");
    }
  }

  /**
   * engine -e removeTechPackInPriorityQueue DC_E_MGW DC_E_SGSN should be
   * allowed by checkNumberOfArguments().
   */
  @Test
  public void testCheckNumberOfArguments_twoTPs() {
    /**
     * Set up array of arguments. For the command: engine -e
     * removeTechPackInPriorityQueue DC_E_MGW DC_E_SGSN Only
     * removeTechPackInPriorityQueue DC_E_MGW DC_E_SGSN will be passed in as
     * arguments. This should be allowed by checkNumberOfArguments().
     */
    final String[] args = new String[3];
    args[0] = "removeTechPackInPriorityQueue";
    args[1] = "DC_E_MGW";
    args[2] = "DC_E_SGSN";

    // Set up test instance. Override createNewEngineAdmin() to return the mock
    // object:
    RemoveTechPacksInPriorityQueueCommand test = new RemoveTechPacksInPriorityQueueCommand(args);

    try {
      test.checkNumberOfArguments();
    } catch (InvalidArgumentsException e) {
      fail("checkNumberOfArguments() should allow 2 or more arguments");
    }
  }

  /**
   * engine -e removeTechPackInPriorityQueue should be allowed by
   * checkNumberOfArguments().
   * 
   * @throws InvalidArgumentsException
   */
  @Test(expected = InvalidArgumentsException.class)
  public void testCheckNumberOfArguments_noTPSpecified() throws InvalidArgumentsException {
    /**
     * Set up array of arguments. For the command: engine -e
     * removeTechPackInPriorityQueue Only the command
     * removeTechPackInPriorityQueue will be passed in as an argument.
     * checkNumberOfArguments() should fail.
     */
    final String[] args = new String[1];
    args[0] = "removeTechPackInPriorityQueue";

    // Set up test instance. Override createNewEngineAdmin() to return the mock
    // object:
    RemoveTechPacksInPriorityQueueCommand test = new RemoveTechPacksInPriorityQueueCommand(args);

    test.checkNumberOfArguments();
  }

  /**
   * checkAndConvertArgumentTypes should convert the array of tech packs in the
   * command args to a list, but shouldn't keep the command name.
   * 
   * @throws InvalidArgumentsException
   */
  @Test
  public void testCheckAndConvertArgumentTypes() throws InvalidArgumentsException {
    final String[] args = new String[3];
    args[0] = "removeTechPackInPriorityQueue";
    args[1] = "DC_E_MGW";
    args[2] = "DC_E_SGSN";

    // Set up test instance. Override createNewEngineAdmin() to return the mock
    // object:
    RemoveTechPacksInPriorityQueueCommand test = new RemoveTechPacksInPriorityQueueCommand(args);

    test.checkAndConvertArgumentTypes();
    Assert.assertNotNull("List of arguments should not be null after converting", test.getTechPackNames());
    Assert.assertFalse("List of arguments should not include the command after converting", test.getTechPackNames()
        .contains("removeTechPackInPriorityQueue"));
  }

  @Test
  public void testGetUsageMessage() {
    final String[] args = new String[3];
    args[0] = "removeTechPackInPriorityQueue";
    args[1] = "DC_E_MGW";
    args[2] = "DC_E_SGSN";
    RemoveTechPacksInPriorityQueueCommand test = new RemoveTechPacksInPriorityQueueCommand(args);
    Assert.assertNotNull("Usage message should not be null", test.getUsageMessage());
  }

}
