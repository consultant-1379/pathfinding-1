package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

public class RestoreCommandTest extends BaseMock {

  EngineAdmin mockedEngineAdmin;

  @Before
  public void setUp() {
    mockedEngineAdmin = context.mock(EngineAdmin.class);
  }

  @Test
  public void testPerformCommand() throws NumberFormatException, Exception {
    final String restore = "restore";
    final String techPackName = "techPack";
    final String regex = "Event*";
    final String restoreStartDate = "2010:07:19";
    final String restoreEndDate = "2010:07:21";
    final String[] validNumberArguments = new String[] { restore, techPackName, regex, restoreStartDate,
 restoreEndDate };
    final Command command = new StubbedRestoreCommand(validNumberArguments);
    expectRestoreCommandOnEngineAdmin(techPackName, regex, restoreStartDate, restoreEndDate);
    command.validateArguments();
    command.performCommand();
  }

  private void expectRestoreCommandOnEngineAdmin(final String techpackName, final String measurementType,
      final String fromDate, final String toDate) throws Exception {
    context.checking(new Expectations() {

      {
        one(mockedEngineAdmin).restore(techpackName, measurementType, fromDate, toDate, null);
      }
    });

  }

  @Test
  public void testInvalidDataFormat1() {
    final String[] invalidDataFormatArguments = new String[] { "restore", "techpackname", "regex", "2010:21:07",
        "2010:07:21" };
    try {
      new RestoreCommand(invalidDataFormatArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(
          e.getMessage(),
          is("Invalid Date format, usage: engine -e restore TechPackName(String) MeasurementType(String)|ALL RestoreStartDate(yyyy:MM:dd) RestoreEndDate(yyyy:MM:dd)"));
    }
  }

  @Test
  public void testInvalidDataFormat2() {
    final String[] invalidDataFormatArguments = new String[] { "restore", "techpackname", "regex", "2010:07:21",
        "2010:21:07" };
    try {
      new RestoreCommand(invalidDataFormatArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(
          e.getMessage(),
          is("Invalid Date format, usage: engine -e restore TechPackName(String) MeasurementType(String)|ALL RestoreStartDate(yyyy:MM:dd) RestoreEndDate(yyyy:MM:dd)"));
    }
  }

  @Test
  public void testInvalidNumberOfArguments_4() {
    final String[] invalidNumberOfArguments = new String[] { "restore", "techpackname", "regex", "2010:07:19" };
    try {
      new RestoreCommand(invalidNumberOfArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(
          e.getMessage(),
          is("Incorrect number of arguments supplied, usage: engine -e restore TechPackName(String) MeasurementType(String)|ALL RestoreStartDate(yyyy:MM:dd) RestoreEndDate(yyyy:MM:dd)"));
    }
  }

  @Test
  public void testInvalidNumberOfArguments_6() {
    final String[] invalidNumberOfArguments = new String[] { "restore", "techpackname", "regex", "2010:07:19",
        "2010:21:07", "extra" };
    try {
      new RestoreCommand(invalidNumberOfArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(
          e.getMessage(),
          is("Incorrect number of arguments supplied, usage: engine -e restore TechPackName(String) MeasurementType(String)|ALL RestoreStartDate(yyyy:MM:dd) RestoreEndDate(yyyy:MM:dd)"));
    }
  }

  class StubbedRestoreCommand extends RestoreCommand {

    /**
     * @param args
     */
    public StubbedRestoreCommand(final String[] args) {
      super(args);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.distocraft.dc5000.etl.engine.main.engineadmincommands.RestoreCommand
     * #createNewEngineAdmin()
     */
    @Override
    protected EngineAdmin createNewEngineAdmin() {
      return mockedEngineAdmin;
    }

  }

}
