package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

public class GetLatestTableNamesForRawEventsCommandTest extends BaseMock {

  EngineAdmin mockedEngineAdmin;

  @Before
  public void setUp() {
    mockedEngineAdmin = context.mock(EngineAdmin.class);
  }

  @Test
  public void testPerformCommandForViewName() throws NumberFormatException, Exception {
    final String viewName = "viewName123";
    final String[] validNumberArguments = new String[] { "getLatestTableNamesForRawEvents", viewName };
    final Command command = new StubbedGetLatestTableNamesForRawEventsCommand(validNumberArguments);
    expectEnableTechPackCommandOnEngineAdmin(viewName);
    command.validateArguments();
    command.performCommand();
  }

  private void expectEnableTechPackCommandOnEngineAdmin(final String viewName) throws Exception {
    context.checking(new Expectations() {

      {
        one(mockedEngineAdmin).getLatestTableNamesForRawEvents(viewName);
      }
    });

  }

  @Test
  public void testCheckArgumentsWith3Arguments() {
    final String[] invalidNumberArguments = new String[] { "onearg", "onearg", "onearg" };
    try {
      new GetLatestTableNamesForRawEventsCommand(invalidNumberArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(e.getMessage(),
          is("Incorrect number of arguments supplied, usage: engine -e getLatestTableNamesForRawEvents viewName(String)"));
    }
  }

  @Test
  public void testCheckArgumentsWith1Argument() {
    final String[] invalidNumberArguments = new String[] { "onearg" };
    try {
      new GetLatestTableNamesForRawEventsCommand(invalidNumberArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(
          e.getMessage(),
          is("Incorrect number of arguments supplied, usage: engine -e getLatestTableNamesForRawEvents viewName(String)"));
    }
  }

  class StubbedGetLatestTableNamesForRawEventsCommand extends GetLatestTableNamesForRawEventsCommand {

    /**
     * @param args
     */
    public StubbedGetLatestTableNamesForRawEventsCommand(final String[] args) {
      super(args);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.distocraft.dc5000.etl.engine.main.engineadmincommands.UnenableSetCommand
     * #createNewEngineAdmin()
     */
    @Override
    protected EngineAdmin createNewEngineAdmin() {
      return mockedEngineAdmin;
    }

  }
}
