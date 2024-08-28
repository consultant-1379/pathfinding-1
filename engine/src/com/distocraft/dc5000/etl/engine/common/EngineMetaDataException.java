package com.distocraft.dc5000.etl.engine.common;

import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;

/**
 * This class constructs different types of EngineExeptions.
 * EngineExeptionHandler is built to parse the information from the Exceptions
 * to show it user friendly.
 * 
 * original author Pekka Kaarela, modified to Dagger Engine project Jukka
 * Jaaheimo
 * 
 * @author $Author: savinen $
 * @since JDK1.1
 */
public class EngineMetaDataException extends EngineBaseException {

	/**
	 * Constructor with error message, non internationalized information and
	 * nested exception
	 * 
	 * @param message
	 *          error message
	 * @param params
	 *          parameters e.g. filename, path
	 * @param nestedException
	 *          Nested exception
	 * @param String
	 *          methodName
	 */
	public EngineMetaDataException(final String message, final String[] params, final Throwable nestedException,
			final String methodName) {
		super(message);
		this.nestedException = nestedException;
		this.errorMessage = message;
		this.params = params;
		this.methodName = methodName;
		this.errorType = EngineConstants.ERR_TYPE_DEFINITION;
	}

	/**
	 * Constructor with error message, non internationalized information and
	 * nested exception
	 * 
	 * @param message
	 *          error message
	 * @param nestedException
	 *          Nested exception
	 * @param String
	 *          methodName
	 */
	public EngineMetaDataException(final String message, final Throwable nestedException, final String methodName) {
		super(message);
		this.nestedException = nestedException;
		this.errorMessage = message;
		this.methodName = methodName;
		this.errorType = EngineConstants.ERR_TYPE_DEFINITION;
	}

	/**
	 * Constructor with error message, non internationalized information and
	 * nested exception
	 * 
	 * @param message
	 *          error message
	 * @param params
	 *          parameters e.g. filename, path
	 * @param nestedException
	 *          Nested exception
	 * @param String
	 *          methodName
	 */
	public EngineMetaDataException(final String message, final String[] params, final Throwable nestedException,
			final TransferActionBase trActionBase, final String methodName) {
		super(message);
		this.nestedException = nestedException;
		this.errorMessage = message;
		this.params = params;
		this.methodName = methodName;
		this.trActionBase = trActionBase;
		this.errorType = EngineConstants.ERR_TYPE_DEFINITION;
	}

	/**
	 * Constructor with error message, non internationalized information and
	 * nested exception
	 * 
	 * @param message
	 *          error message
	 * @param nestedException
	 *          Nested exception
	 * @param String
	 *          methodName
	 */
	public EngineMetaDataException(final String message, final Throwable nestedException,
			final TransferActionBase trActionBase, final String methodName) {
		super(message);
		this.nestedException = nestedException;
		this.errorMessage = message;
		this.methodName = methodName;
		this.trActionBase = trActionBase;
		this.errorType = EngineConstants.ERR_TYPE_DEFINITION;
	}

}
