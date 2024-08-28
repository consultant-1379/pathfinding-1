/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.io.ByteArrayOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.StringTokenizer;

import com.distocraft.dc5000.etl.engine.main.EngineAdmin;

/**
 * Base command for all CLI engine -e commands For each possible command that
 * the user can enter with engine -e, there should be a corresponding subclass
 * of Command (see EngineAdmin for the mapping between the commands and the
 * subclasses.
 * 
 * Each Command subclass must: 1) provide logic to validate the number of
 * arguments 2) for any integer/long arguments, ensure that the arguments are of
 * type integer/long 3) perform the actual command (usually on the EngineAdmin
 * instance)
 * 
 * @author eemecoy
 * 
 */
public abstract class Command {

  final String[] arguments;

  /**
   * simple constructor - note, no more logic should be added here - this
   * constructor should be kept as simple as possible, as it is called using
   * reflection
   * 
   * @param args
   *          command line arguments that the user has entered (this includes
   *          the actual name of the command as the first argument)
   */
  public Command(final String[] args) {
    arguments = args;
  }

  public Command() {
    arguments = null;
  }

  /**
   * Check that the number of arguments is correct Checks the type of
   * integer/long arguments Assigns arguments for future use
   * 
   * @throws InvalidArgumentsException
   */
  public void validateArguments() throws InvalidArgumentsException {
    checkNumberOfArguments();
    checkAndConvertArgumentTypes();
  }

  void checkNumberOfArguments() throws InvalidArgumentsException {
    final int correctArgumentsLength = getCorrectArgumentsLength();
    if (arguments.length < correctArgumentsLength || arguments.length > correctArgumentsLength) {
      throw new InvalidArgumentsException("Incorrect number of arguments supplied, usage: engine -e "
          + getUsageMessage());
    }
  }

  abstract void checkAndConvertArgumentTypes() throws InvalidArgumentsException;

  long convertArgumentToLong(final String argumentAsString) throws InvalidArgumentsException {
    try {
      return Long.parseLong(argumentAsString);
    } catch (final NumberFormatException e) {
      throw new InvalidArgumentsException("Invalid arguments type, usage: engine -e " + getUsageMessage());
    }
  }

  int convertArgumentToInteger(final String argumentAsString) throws InvalidArgumentsException {
    try {
      return Integer.parseInt(argumentAsString);
    } catch (final NumberFormatException e) {
      throw new InvalidArgumentsException("Invalid arguments type, usage: engine -e " + getUsageMessage());
    }
  }

  /**
   * 
   * @return the correct usage for this command eg startSet setName(string)
   */
  abstract String getUsageMessage();

  /**
   * perform the actual command that this Command represents
   * 
   * @throws Exception
   */
  public abstract void performCommand() throws Exception;

  /**
   * extracted out for unit test
   */
  protected EngineAdmin createNewEngineAdmin() {
    return new EngineAdmin();
  }

  protected abstract int getCorrectArgumentsLength();

  /**
   * The input string can be a property setting formatted like this: key=value
   * Or it can be more than one property set like this: key1=value1 key2=value2
   * (or alternatively key1=value1=key2=value2 can be used if there is a problem
   * submitting space character in command argument) Any number of pairs can be
   * submitted.
   * 
   * @throws InvalidArgumentsException
   */
  String createPropertyString(final String str) throws InvalidArgumentsException {

    String result = "";

    try {

      final Properties prop = new Properties();
      final StringTokenizer st = new StringTokenizer(str, "= ");
      while (st.hasMoreTokens()) { // Add each key value pair to properties
                                   // object
        final String key = st.nextToken();
        String value = st.nextToken();
        // TR HO88115
        // change aggDate value to long
        if (key.equals("aggDate")) {
          final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          try {
            if (sdf.parse(value).getTime() < 0) {
              throw new ParseException("aggDate Value should be in 'yyyy-MM-dd'", 0);
            }
            value = Long.toString(sdf.parse(value).getTime());

          } catch (ParseException e) {
            throw new NumberFormatException("Please check the aggDate value: " + value
                + ". It should be in 'yyyy-MM-dd' format");
          }
        }
        prop.setProperty(key.trim(), value.trim());
      }

      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      prop.store(baos, "");

      result = baos.toString();

    } catch (final NumberFormatException e) {
      throw new InvalidArgumentsException(e.getMessage());
    } catch (final Exception e) {
      System.out.println("Warning: Invalid argument Schedule. Usage: engine -e " + getUsageMessage());
      System.out.println("Using default value for Schedule to execute the command.");
    }

    return result;
  }
}
