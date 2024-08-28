/*------------------------------------------------------------------------
 *
 *
 *      COPYRIGHT (C)                   ERICSSON RADIO SYSTEMS AB, Sweden
 *
 *      The  copyright  to  the document(s) herein  is  the property of
 *      Ericsson Radio Systems AB, Sweden.
 *
 *      The document(s) may be used  and/or copied only with the written
 *      permission from Ericsson Radio Systems AB  or in accordance with
 *      the terms  and conditions  stipulated in the  agreement/contract
 *      under which the document(s) have been supplied.
 *
 *------------------------------------------------------------------------
 */

package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * @author eeimho
 * 
 */

public class RestoreCommand extends Command {

  private String techPackName;
  private String measurementType;
  private String fromDate;
  private String toDate;
  // Allows restore to be run without asking a question
  private String autoRun;

  public RestoreCommand(final String[] args) {
    super(args);
  }

  /*
   * (non-Javadoc)
   * 
   * @seecom.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * checkAndConvertArgumentTypes()
   */
  @Override
  void checkAndConvertArgumentTypes() throws InvalidArgumentsException {
    techPackName = arguments[1];
    measurementType = arguments[2];
    fromDate = arguments[3];
    toDate = arguments[4];

    if (arguments.length == 6) {
      autoRun = arguments[5];
    }

    if (!isValidDate(fromDate) || !isValidDate(toDate)) {
      throw new InvalidArgumentsException("Invalid Date format, usage: engine -e " + getUsageMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @seecom.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * getCorrectArgumentsLength()
   */
  @Override
  protected int getCorrectArgumentsLength() {
    if (arguments.length == 6 && arguments[5].equals("autoRun")) {
      return 6;
    }
    return 5;
  }

  /*
   * (non-Javadoc)
   * 
   * @seecom.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * getUsageMessage()
   */
  @Override
  String getUsageMessage() {
    return "restore TechPackName(String) MeasurementType(String)|ALL RestoreStartDate(yyyy:MM:dd) RestoreEndDate(yyyy:MM:dd)";
  }

  /**
   * Date validation using SimpleDateFormat it will take a string and make sure
   * it's in the proper format as defined by you, and it will also make sure
   * that it's a legal date
   * 
   * @param date
   * @return
   */
  public boolean isValidDate(final String date) {
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy:MM:dd");
    Date testDate = null;
    try {
      testDate = sdf.parse(date);
    } catch (ParseException e) {
      return false;
    }
    if (!sdf.format(testDate).equals(date)) {
      return false;
    }
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @seecom.distocraft.dc5000.etl.engine.main.engineadmincommands.Command#
   * performCommand()
   */
  @Override
  public void performCommand() throws NotBoundException, MalformedURLException, RemoteException {
    final EngineAdmin admin = createNewEngineAdmin();
    admin.restore(techPackName, measurementType, fromDate, toDate, autoRun);
  }

}
