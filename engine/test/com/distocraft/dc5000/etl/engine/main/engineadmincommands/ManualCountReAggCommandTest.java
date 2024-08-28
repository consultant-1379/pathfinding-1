package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.jmock.Expectations;
import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.etl.engine.BaseMock;
import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

public class ManualCountReAggCommandTest extends BaseMock {

  EngineAdmin mockedEngineAdmin;

  @Before
  public void setUp() {
    mockedEngineAdmin = context.mock(EngineAdmin.class);
  }

  @Test
  public void testPerformCommandForTechPack() throws NumberFormatException, Exception {
    final String techPackName = "techPack";
    final String minTimeStamp = getTimeStampInCorrectFormat(0L);
    final String maxTimeStamp = getTimeStampInCorrectFormat(System.currentTimeMillis());
    final String intervalName = "DAY";
    final boolean isScheduled = false;

    final String[] validNumberArguments = new String[] { "manualCountReAgg", techPackName, minTimeStamp, maxTimeStamp,
        intervalName, Boolean.toString(isScheduled) };
    final Command command = new StubbedManualCountReAggCommand(validNumberArguments);

    expectManualCountReAgg(techPackName, getTimeStampInToNearestMin(minTimeStamp),
        getTimeStampInToNearestMin(maxTimeStamp), intervalName, isScheduled);
    command.validateArguments();
    command.performCommand();
  }

  @Test
  public void testCheckArgumentsWith5Arguments() {
    final String[] invalidNumberArguments = new String[] { "onearg", "onearg", "onearg", "onearg", "onearg", "onearg" };
    try {
      new ManualCountReAggCommand(invalidNumberArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(e.getMessage(),
          is("Invalid arguments type, usage: engine -e manualCountReAgg techPackName(String) "
          + "minTimestamp((UTC) yyyy:MM:dd:HH:mm) maxTimestamp((UTC) yyyy:MM:dd:HH:mm) intervalName(String) "
          + "isScheduled(TRUE or FALSE)"));
    }
  }

  @Test
  public void testCheckArgumentsWith1Argument() {
    final String[] invalidNumberArguments = new String[] { "onearg" };
    try {
      new ManualCountReAggCommand(invalidNumberArguments).validateArguments();
      fail("Exception should have been thrown");
    } catch (final InvalidArgumentsException e) {
      assertThat(e.getMessage(),
          is("Incorrect number of arguments supplied, usage: engine -e manualCountReAgg techPackName(String) "
              + "minTimestamp((UTC) yyyy:MM:dd:HH:mm) maxTimestamp((UTC) yyyy:MM:dd:HH:mm) intervalName(String) "
              + "isScheduled(TRUE or FALSE)"));
    }
  }

  private String getTimeStampInCorrectFormat(final long timeInMillis) {
    final Timestamp timeStamp = new Timestamp(timeInMillis);
    final SimpleDateFormat dbUTCDateTimeFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm", Locale.getDefault());
    dbUTCDateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return dbUTCDateTimeFormat.format(timeStamp);
  }

  private Timestamp getTimeStampInToNearestMin(final String timeStamp) throws ParseException {
    final SimpleDateFormat dbUTCDateTimeFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm", Locale.getDefault());
    dbUTCDateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return new Timestamp(dbUTCDateTimeFormat.parse(timeStamp).getTime());
  }

  private void expectManualCountReAgg(final String techpackName, final Timestamp minTimeStamp,
      final Timestamp maxTimeStamp, final String intervalName, final boolean isScheduled) throws Exception {

    context.checking(new Expectations() {

      {
        one(mockedEngineAdmin).manualCountReAgg(techpackName, minTimeStamp, maxTimeStamp, intervalName, isScheduled);
      }
    });

  }

  class StubbedManualCountReAggCommand extends ManualCountReAggCommand {

    /**
     * @param args
     */
    public StubbedManualCountReAggCommand(final String[] args) {
      super(args);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.distocraft.dc5000.etl.engine.main.engineadmincommands.UndisableSetCommand
     * #createNewEngineAdmin()
     */
    @Override
    protected EngineAdmin createNewEngineAdmin() {
      return mockedEngineAdmin;
    }

  }
}
