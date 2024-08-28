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
public class RefreshDBLookupsCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommandNotSpecifyingTableName() throws NumberFormatException, Exception {
        final String[] validNumberArguments = new String[] { "refreshDBLookups" };
        final Command command = new StubbedRefreshDBLookupsCommand(validNumberArguments);
        expectRefreshDBLookupsCommandOnEngineAdmin(null);
        command.validateArguments();
        command.performCommand();
    }

    @Test
    public void testPerformCommandSpecifyingTableName() throws NumberFormatException, Exception {
        final String tableName = "some table";
        final String[] validNumberArguments = new String[] { "refreshDBLookups", tableName };
        final Command command = new StubbedRefreshDBLookupsCommand(validNumberArguments);
        expectRefreshDBLookupsCommandOnEngineAdmin(tableName);
        command.validateArguments();
        command.performCommand();
    }

    private void expectRefreshDBLookupsCommandOnEngineAdmin(final String tableName) throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).refreshDBLookups(tableName);
            }
        });

    }

    @Test
    public void testCheckArgumentsWithTooManyArguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "threearg" };
        try {
            new RefreshDBLookupsCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e refreshDBLookups tableName(string) or engine -e refreshDBLookups"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments2DoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "refreshDBLookups" };
        new RefreshDBLookupsCommand(validNumberArguments).validateArguments();
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArguments3DoesntThrowException() throws InvalidArgumentsException {

        final String[] validNumberArguments = new String[] { "refreshDBLookups", "someTable" };
        new RefreshDBLookupsCommand(validNumberArguments).validateArguments();
    }

    class StubbedRefreshDBLookupsCommand extends RefreshDBLookupsCommand {

        /**
         * @param args
         */
        public StubbedRefreshDBLookupsCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.UnrefreshDBLookupsCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
