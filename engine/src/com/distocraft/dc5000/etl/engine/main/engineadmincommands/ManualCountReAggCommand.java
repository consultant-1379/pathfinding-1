package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

public class ManualCountReAggCommand extends Command {

  String techPackName;

  Timestamp minTimestamp;

  Timestamp maxTimestamp;

  String intervalName;

  boolean isScheduled;

  private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

  public ManualCountReAggCommand(final String[] args) {
    super(args);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * checkArgumentTypes()
   */
  @Override
  void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
    techPackName = arguments[1];
    minTimestamp = convertArgumentToTimeStamp(arguments[2]);
    maxTimestamp = convertArgumentToTimeStamp(arguments[3]);
    intervalName = arguments[4];
    isScheduled = convertArgumentToBoolean(arguments[5]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * getCorrectArgumentsLength()
   */
  @Override
  protected int getCorrectArgumentsLength() {
    return 6;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * performCommand()
   */
  @Override
  public void performCommand() throws NumberFormatException {
    final EngineAdmin admin = createNewEngineAdmin();
    try {
      admin.manualCountReAgg(techPackName, minTimestamp, maxTimestamp, intervalName, isScheduled);
    } catch (final Exception e) {
      System.out.println("Invalid techPackName, startTime, endTime or intervalName entered.\n" + e.getMessage());
      System.exit(0);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * getUsageMessage()
   */
  @Override
  String getUsageMessage() {
    return "manualCountReAgg techPackName(String) minTimestamp((UTC) yyyy:MM:dd:HH:mm) maxTimestamp((UTC) yyyy:MM:dd:HH:mm) intervalName(String) isScheduled(TRUE or FALSE)";
  }

  private Timestamp convertArgumentToTimeStamp(final String argumentAsString) throws InvalidArgumentsException {
    try {
      final SimpleDateFormat dbUTCDateTimeFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm", Locale.getDefault());
      dbUTCDateTimeFormat.setTimeZone(UTC_TIME_ZONE);
      final long timeInMilliSecs = dbUTCDateTimeFormat.parse(argumentAsString).getTime();
      return new Timestamp(timeInMilliSecs);
    } catch (final ParseException e) {
      throw new InvalidArgumentsException("Invalid arguments type, usage: engine -e " + getUsageMessage());
    }
  }

  private boolean convertArgumentToBoolean(final String argumentAsString) throws InvalidArgumentsException {
    if (argumentAsString.equalsIgnoreCase("true") || argumentAsString.equalsIgnoreCase("false")) {
      return Boolean.parseBoolean(argumentAsString);
    }

    throw new InvalidArgumentsException("Invalid arguments type, usage: engine -e " + getUsageMessage());
  }
}
