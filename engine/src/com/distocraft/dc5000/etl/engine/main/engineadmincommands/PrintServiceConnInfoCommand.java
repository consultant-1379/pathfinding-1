package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * User: eeipca
 * Date: 17/05/12
 * Time: 11:32
 */
public class PrintServiceConnInfoCommand extends Command {
  /**
   * @param args
   */
  public PrintServiceConnInfoCommand(final String[] args) {
    super(args);
  }

  @Override
  void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
  }

  @Override
  public String getUsageMessage() {
    return null;
  }

  @Override
  public void performCommand() throws Exception {
    final EngineAdmin admin = createNewEngineAdmin();
    admin.printServiceInfo();
  }

  @Override
  protected int getCorrectArgumentsLength() {
    return 1;
  }
}
