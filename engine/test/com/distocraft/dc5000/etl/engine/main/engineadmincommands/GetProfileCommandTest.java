/**
 * 
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eheijun
 * 
 */
public class GetProfileCommandTest extends BaseMock {

  EngineAdmin mockedEngineAdmin;

  @Before
  public void setUp() {
    mockedEngineAdmin = context.mock(EngineAdmin.class);
  }

  /**
   * Test method for
   * {@link com.distocraft.dc5000.etl.engine.main.engineadmincommands.GetProfileCommand#performCommand()}.
   * 
   * @throws Exception
   */
  @Test
  public void testPerformCommand() throws Exception {
    final String[] validNumberArguments = new String[] { "getProfile" };
    final Command command = new StubbedGetProfileCommand(validNumberArguments);
    expectGetProfileOnEngineAdmin();
    command.validateArguments();
    command.performCommand();
  }

  private void expectGetProfileOnEngineAdmin() throws Exception {
    context.checking(new Expectations() {
      {
        one(mockedEngineAdmin).status();
      }
    });
  }

  @Test
  public void testCheckArgumentsWith2Arguments() {
      final String[] invalidNumberArguments = new String[] { "onearg", "twoarg" };
      try {
          new GetProfileCommand(invalidNumberArguments).validateArguments();
          fail("Exception should have been thrown");
      } catch (final InvalidArgumentsException e) {
          assertThat(e.getMessage(), is("Incorrect number of arguments supplied, usage: engine -e currentProfile"));
      }
  }
  
  @Test

  public void testCheckArgumentsWithCorrectNumArgumentsDoesntThrowException() throws InvalidArgumentsException {
      final String[] validNumberArguments = new String[] { "currentProfile" };
      new GetProfileCommand(validNumberArguments).validateArguments();

  }
  
  class StubbedGetProfileCommand extends StatusCommand {
    /**
     * 
     * @param args
     */
    public StubbedGetProfileCommand(final String[] args) {
      super(args);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.StatusCommand#createNewEngineAdmin()
     */

    @Override
    protected EngineAdmin createNewEngineAdmin() {
      return mockedEngineAdmin;
    }

  }
}
