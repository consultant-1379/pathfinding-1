package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;


public class GetTableNamesForRawEventsCommand extends Command {

  String viewName;

  Timestamp startTime;

  Timestamp endTime;

  private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

  public GetTableNamesForRawEventsCommand(final String[] args) {
    super(args);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#checkArgumentTypes()
   */
  @Override
  void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
    viewName = arguments[1];
    startTime = convertArgumentToTimeStamp(arguments[2]);
    endTime = convertArgumentToTimeStamp(arguments[3]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getCorrectArgumentsLength()
   */
  @Override
  protected int getCorrectArgumentsLength() {
    return 4;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#performCommand()
   */
  @Override
  public void performCommand() throws NumberFormatException {
    final EngineAdmin admin = createNewEngineAdmin();
    try {
      admin.getTableNamesForRawEvents(viewName, startTime, endTime);
    } catch (final Exception e) {
      System.out.println("Invalid viewName or startTime or endTime entered ");
      System.exit(0);
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#getUsageMessage()
   */
  @Override
  String getUsageMessage() {
    return "getTableNamesForRawEvents viewName(String) startTime((UTC) yyyy:MM:dd:HH:mm) endTime((UTC) yyyy:MM:dd:HH:mm)";
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

}
