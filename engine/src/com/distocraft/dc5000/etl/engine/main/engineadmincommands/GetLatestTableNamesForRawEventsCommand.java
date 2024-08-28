package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

public class GetLatestTableNamesForRawEventsCommand extends Command {

  String viewName;

  public GetLatestTableNamesForRawEventsCommand(final String[] args) {
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
    viewName = arguments[1];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * getCorrectArgumentsLength()
   */
  @Override
  protected int getCorrectArgumentsLength() {
    return 2;
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
      admin.getLatestTableNamesForRawEvents(viewName);
    } catch (final Exception e) {
      System.out.println("Invalid viewName entered ");
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
    return "getLatestTableNamesForRawEvents viewName(String)";
  }
}
