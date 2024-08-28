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
public class StartSetsCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final String techPackName = "some tech pack";
        final String setName = "some set";
        final String numberTimesToRunSet = "4";
        final String[] validNumberArguments = new String[] { "-e", techPackName, setName, numberTimesToRunSet };
        final Command startSetsCommand = new StubbedStartSetsCommand(validNumberArguments);
        expectStartSetOnEngineAdmin(techPackName, setName, Integer.parseInt(numberTimesToRunSet));
        startSetsCommand.validateArguments();
        startSetsCommand.performCommand();
    }

    private void expectStartSetOnEngineAdmin(final String techPackName, final String setName,
            final int numberTimesToRunSet) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).startSets(techPackName, setName, numberTimesToRunSet);
            }
        });

    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsButInvalidTypes() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg",
                "this should be a number" };
        try {
            new StartSetsCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(),
                    is("Invalid arguments type, usage: engine -e startSets techPacks(string) sets(string) times(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWith3Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new StartSetsCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSets techPacks(string) sets(string) times(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWith5Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg", "fourarg", "fivearg" };
        try {
            new StartSetsCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSets techPacks(string) sets(string) times(long)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "onearg", "twoarg", "threearg", "4" };
        new StartSetsCommand(validNumberArguments).validateArguments();
    }

    class StubbedStartSetsCommand extends StartSetsCommand {

        /**
         * @param args
         */
        public StubbedStartSetsCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetsCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
