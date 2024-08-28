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
public class HoldSetInPriorityQueueCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String setId = "4";
        final String[] validNumberArguments = new String[] { "-e", setId };
        final Command command = new StubbedHoldSetInPriorityQueueCommand(validNumberArguments);
        command.validateArguments();
        expectHoldSetInPriorityQueueOnEngineAdmin(Integer.parseInt(setId));
        command.performCommand();
    }

    private void expectHoldSetInPriorityQueueOnEngineAdmin(final int setId) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).holdSetInPriorityQueue(setId);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith1Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg" };
        try {
            new HoldSetInPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e holdSetInPriorityQueue setId(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWith3Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new HoldSetInPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e holdSetInPriorityQueue setId(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsButOfWrongType() {
        final String[] validNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new HoldSetInPriorityQueueCommand(validNumberArguments).validateArguments();
            fail("Should have thrown an exception");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Invalid arguments type, usage: engine -e holdSetInPriorityQueue setId(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "onearg", "4" };
        new HoldSetInPriorityQueueCommand(validNumberArguments).validateArguments();
    }

    class StubbedHoldSetInPriorityQueueCommand extends HoldSetInPriorityQueueCommand {

        /**
         * @param args
         */
        public StubbedHoldSetInPriorityQueueCommand(final String[] args) {
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
