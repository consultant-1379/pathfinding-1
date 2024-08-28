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
public class RemoveSetFromPriorityQueueCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String setId = "4";
        final String[] validNumberArguments = new String[] { "-e", setId };
        final Command command = new StubbedRemoveSetFromPriorityQueueCommand(validNumberArguments);
        expectRemoveSetFromPriorityQueueOnEngineAdmin(Integer.parseInt(setId));
        command.validateArguments();
        command.performCommand();
    }

    private void expectRemoveSetFromPriorityQueueOnEngineAdmin(final int setId) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).removeSetFromPriorityQueue(setId);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith1Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg" };
        try {
            new RemoveSetFromPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e removeSetFromPriorityQueue long"));
        }
    }

    @Test
    public void testCheckArgumentsWith3Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new RemoveSetFromPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e removeSetFromPriorityQueue long"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsButInvalidTypes() {
        final String[] invalidArguments = new String[] { "onearg", "twoarg" };
        try {
            new RemoveSetFromPriorityQueueCommand(invalidArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(), is("Invalid arguments type, usage: engine -e removeSetFromPriorityQueue long"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "4", "7" };
        new RemoveSetFromPriorityQueueCommand(validNumberArguments).validateArguments();
    }

    class StubbedRemoveSetFromPriorityQueueCommand extends RemoveSetFromPriorityQueueCommand {

        /**
         * @param args
         */
        public StubbedRemoveSetFromPriorityQueueCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.RemoveSetFromPriorityQueueCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
