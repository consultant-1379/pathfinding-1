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
public class ChangeSetPriorityInPriorityQueueCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String setId = "4";
        final String newPriority = "34";
        final String[] validNumberArguments = new String[] { "-e", setId, newPriority };
        final Command command = new StubbedChangeSetPriorityInPriorityQueue(validNumberArguments);
        expectChangeSetPriorityInPriorityQueueOnEngineAdmin(Integer.parseInt(setId), Integer.parseInt(newPriority));
        command.validateArguments();
        command.performCommand();
    }

    private void expectChangeSetPriorityInPriorityQueueOnEngineAdmin(final int setId, final int newPriority)
            throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).changeSetPriorityInPriorityQueue(setId, newPriority);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg" };
        try {
            new ChangeSetPriorityInPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e changeSetPriorityInPriorityQueue setId(long) newPriority(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWith4Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg", "fourarg" };
        try {
            new ChangeSetPriorityInPriorityQueueCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e changeSetPriorityInPriorityQueue setId(long) newPriority(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsButOfWrongType() {
        final String[] validNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new ChangeSetPriorityInPriorityQueueCommand(validNumberArguments).validateArguments();
            fail("Should have thrown an exception");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Invalid arguments type, usage: engine -e changeSetPriorityInPriorityQueue setId(long) newPriority(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "onearg", "4", "3" };
        new ChangeSetPriorityInPriorityQueueCommand(validNumberArguments).validateArguments();
    }

    class StubbedChangeSetPriorityInPriorityQueue extends ChangeSetPriorityInPriorityQueueCommand {

        /**
         * @param args
         */
        public StubbedChangeSetPriorityInPriorityQueue(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeSetPriorityInPriorityQueue#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
