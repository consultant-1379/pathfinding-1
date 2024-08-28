/**
 * 
 */
package com.ericsson.eniq.enminterworking;

/**
 * @author xnagdas
 *
 */

import com.ericsson.eniq.flssymlink.fls.FlsProfileHandler;

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
	      throw new InvalidArgumentsException("Incorrect number of arguments supplied, usage: /eniq/admin/bin/fls -e "
	          + getUsageMessage());
	    }
	  }

	  abstract void checkAndConvertArgumentTypes() throws InvalidArgumentsException;

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
	  protected FlsProfileHandler createNewFLSAdmin() {
	    return new FlsProfileHandler();
	  }

	  protected abstract int getCorrectArgumentsLength();

	}
