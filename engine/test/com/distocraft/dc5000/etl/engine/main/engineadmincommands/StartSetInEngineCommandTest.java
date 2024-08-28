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
public class StartSetInEngineCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    protected String ip = "some ip";

    protected String username = "a user";

    protected String passwd = "some password";

    protected String driver = "a driver";

    protected String coll = " a collection";

    protected String set = " a set";

    private final String[] validNumberArguments = new String[] { "startSetInEngine", ip, username, passwd, driver,
            coll, set };

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
    }

    @Test
    public void testPerformCommand() throws NumberFormatException, Exception {
        final Command command = new StubbedStartSetInEngineCommand(validNumberArguments);
        expectStartSetWith6ArgumentsOnEngineAdmin();
        command.validateArguments();
        command.performCommand();
    }

    private void expectStartSetWith6ArgumentsOnEngineAdmin() throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).startSet(ip, username, passwd, driver, coll, set);
            }
        });

    }

    @Test
    public void testCheckArgumentsWith6Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "anarg", "anarg", "anarg", "anarg" };
        try {
            new StartSetInEngineCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSetInEngine ip(string) username(string) password(string) driver(string) collection(string) set(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWith8Arguments() {
        final String[] invalidNumberArguments = new String[] { "onearg", "twoarg", "anarg", "anarg", "anarg", "anarg",
                "anarg", "anarg" };
        try {
            new StartSetInEngineCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(
                    e.getMessage(),
                    is("Incorrect number of arguments supplied, usage: engine -e startSetInEngine ip(string) username(string) password(string) driver(string) collection(string) set(string)"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {

        new StartSetInEngineCommand(validNumberArguments).validateArguments();
    }

    class StubbedStartSetInEngineCommand extends StartSetInEngineCommand {

        /**
         * @param args
         */
        public StubbedStartSetInEngineCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetInEngineCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

    }

}
