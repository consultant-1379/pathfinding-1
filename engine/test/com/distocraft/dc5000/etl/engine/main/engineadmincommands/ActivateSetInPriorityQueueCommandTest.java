/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class ActivateSetInPriorityQueueCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String setId = "4";
        final String[] validNumberArguments = new String[] { "activateSetInPriorityQueue", setId };
        final Command command = new StubbedActivateSetInPriorityQueueCommand(validNumberArguments);
        expectChangeSetPriorityInPriorityQueueOnEngineAdmin(Integer.parseInt(setId));
        command.validateArguments();
        command.performCommand();
    }

    private void expectChangeSetPriorityInPriorityQueueOnEngineAdmin(final int setId) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).activateSetInPriorityQueue(setId);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith1Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg" };
        try {
            new ActivateSetInPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e activateSetInPriorityQueue setId(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWith3Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new ActivateSetInPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e activateSetInPriorityQueue setId(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsButOfWrongType() {
        final String[] validNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new ActivateSetInPriorityQueueCommand(validNumberArguments).validateArguments();
            fail("Should have thrown an exception");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Invalid arguments type, usage: engine -e activateSetInPriorityQueue setId(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "onearg", "4", "3" };
        new ChangeSetPriorityInPriorityQueueCommand(validNumberArguments).validateArguments();
    }

    class StubbedActivateSetInPriorityQueueCommand extends ActivateSetInPriorityQueueCommand {

        /**
         * @param args
         */
        public StubbedActivateSetInPriorityQueueCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.ActivateSetInPriorityQueue#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
