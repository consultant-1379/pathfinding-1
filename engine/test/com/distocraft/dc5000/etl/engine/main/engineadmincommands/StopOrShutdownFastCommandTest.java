/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eemecoy
 *
 */
public class StopOrShutdownFastCommandTest extends BaseMock {

    EngineAdmin mockedEngineAdmin;

    File mockedDwhDbFullFile;

    Runtime mockedRunTime;

    @Before
    public void setUp() {
        mockedEngineAdmin = context.mock(EngineAdmin.class);
        mockedDwhDbFullFile = context.mock(File.class);
        mockedRunTime = context.mock(Runtime.class);
    }

    @Test
    public void testPerformCommandStopdwhdb_fullFileExists() throws NumberFormatException, Exception {
        final String[] validNumberArguments = new String[] { "stop" };
        final Command stopCommand = new StubbedStopOrShutdownFastCommand(validNumberArguments);
        stopCommand.validateArguments();

        setDwhDbFullFileToExistOrNot(true);
        expectEngineStopOnRuntime();
        stopCommand.performCommand();
    }

    private void expectEngineStopOnRuntime() throws IOException {
        context.checking(new Expectations() {
            {
                one(mockedRunTime).exec("engine stop");
            }
        });

    }

    private void setDwhDbFullFileToExistOrNot(final boolean doesFileExist) {
        context.checking(new Expectations() {
            {
                one(mockedDwhDbFullFile).exists();
                will(returnValue(doesFileExist));
            }
        });

    }

    @Test
    public void testPerformCommandShutdownFast() throws NumberFormatException, Exception {
        final String[] validNumberArguments = new String[] { "shutdown_fast" };
        final Command shutDownFastCommand = new StubbedStopOrShutdownFastCommand(validNumberArguments);
        shutDownFastCommand.validateArguments();

        setDwhDbFullFileToExistOrNot(false);
        expectFastGracefulShutdownOnEngineAdmin();
        shutDownFastCommand.performCommand();

    }

    private void expectFastGracefulShutdownOnEngineAdmin() throws Exception {
        context.checking(new Expectations() {
            {
                one(mockedEngineAdmin).fastGracefulShutdown();
            }
        });

    }

    @Test
    public void testStopCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "stop", "twoarg" };
        try {
            new StubbedStopOrShutdownFastCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(), is("Incorrect number of arguments supplied, usage: engine -e stop"));
        }
    }

    @Test
    public void testShutdownFastCheckArgumentsWith2Arguments() {
        final String[] invalidNumberArguments = new String[] { "shutdown_fast", "twoarg" };
        try {
            new StubbedStopOrShutdownFastCommand(invalidNumberArguments).validateArguments();
            fail("Exception should have been thrown");
        } catch (final InvalidArgumentsException e) {
            assertThat(e.getMessage(), is("Incorrect number of arguments supplied, usage: engine -e shutdown_fast"));
        }
    }

    @Test
    public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
        final String[] validNumberArguments = new String[] { "onearg" };
        new StubbedStopOrShutdownFastCommand(validNumberArguments).validateArguments();
    }

    class StubbedStopOrShutdownFastCommand extends StopOrShutdownFastCommand {

        /**
         * @param args
         */
        public StubbedStopOrShutdownFastCommand(final String[] args) {
            super(args);
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetsCommand#createNewEngineAdmin()
         */
        @Override
        protected EngineAdmin createNewEngineAdmin() {
            return mockedEngineAdmin;
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StopOrShutdownFastCommand#createFileObject(java.lang.String)
         */
        @Override
        File createFileObject(final String fileName) {
            return mockedDwhDbFullFile;
        }

        /* (non-Javadoc)
         * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StopOrShutdownFastCommand#getRunTime()
         */
        @Override
        Runtime getRunTime() {
            return mockedRunTime;
        }

    }

}
