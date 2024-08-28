package com.distocraft.dc5000.etl.engine.common;

import java.sql.SQLException;
import java.text.MessageFormat;

import ssc.rockfactory.RockException;

import com.distocraft.dc5000.etl.engine.plugin.PluginException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;

/**
 * 
 * This class constructs different types of EngineExeptions.
 * EngineExeptionHandler is built to parse the information from the Exceptions
 * to show it user friendly.
 * 
 * original author Pekka Kaarela, modified to Dagger Engine project Jukka
 * Jaaheimo
 * 
 * @author $Author: lemminkainen $
 * @since JDK1.1
 */

public class EngineBaseException extends Exception {

	/** own error message in exception */
	protected String errorMessage = null;

	/** own error message in exception */
	protected String errorType = null;

	/** Nested exception */
	protected Throwable nestedException = null;

	/** method that created the exception */
	protected String methodName = null;

	protected TransferActionBase trActionBase;

	/** array of non internationalized info e.g. filenames and paths */
	protected String[] params = null;

	/**
	 * Default constructor - not allowed to use
	 */
	protected EngineBaseException() {
		super();
	}

	/**
	 * Default constructor - not allowed to use
	 */
	protected EngineBaseException(final String s) {
		super(s);
	}

	/**
	 * Returns the nested exception or null if there is no nested exception
	 * 
	 * @return Throwable Nested exception
	 */
	public Throwable getNestedException() {
		return this.nestedException;
	}

	/**
	 * Returns the parameters of the exception or null if none
	 * 
	 * @return parameters(e.g. filename, path)
	 */

	public String[] getParams() {
		return this.params;
	}

	/**
	 * Returns the method name that created this exception.
	 * 
	 * @return methodName
	 */
	public String getMethodName() {
		Exception e = this;
		String retMethodName = this.methodName;

		while (e != null) {

			if (e instanceof EngineBaseException) {
				retMethodName = ((EngineBaseException) e).methodName;
				e = (Exception) ((EngineBaseException) e).getNestedException();
			} else {
				return retMethodName;
			}

		}
		return retMethodName;
	}

	/**
	 * Returns the error message of the exception or null if none
	 * 
	 * @return error message
	 */
	public String getErrorMessage() {
		if ((this.getParams() != null) && (this.getParams().length > 0)) {
			return MessageFormat.format(this.errorMessage, (Object[])this.getParams());
		} else {
			return this.errorMessage;
		}
	}

	/**
	 * Return the error type of the exception. If in the exception hierarchy there
	 * is a SQLException or RockException, a SQL error type is returned.
	 * 
	 * @return
	 */
	public String getErrorType() {
		Exception e = this;
		String errorTypeText = this.errorType;

		while (e != null) {

			if (e instanceof EngineException) {

				errorTypeText = ((EngineException) e).errorType;
				e = (Exception) ((EngineException) e).getNestedException();
			} else if (e instanceof EngineMetaDataException) {

				errorTypeText = ((EngineMetaDataException) e).errorType;
				e = (Exception) ((EngineMetaDataException) e).getNestedException();
			} else if (e instanceof RemoveDataException) {

				errorTypeText = ((RemoveDataException) e).errorType;
				e = (Exception) ((RemoveDataException) e).getNestedException();
			} else if (e instanceof SQLException) {

				errorTypeText = EngineConstants.ERR_TYPE_SQL;
				e = null;
			} else if (e instanceof RockException) {

				errorTypeText = EngineConstants.ERR_TYPE_SQL;
				e = null;
			} else if (e instanceof PluginException) {

				errorTypeText = EngineConstants.ERR_TYPE_SYSTEM;
				e = (Exception) ((PluginException) e).getNestedException();

			}

			else {

				e = null;
			}

		}
		return errorTypeText;
	}

	/**
	 * Reuturns the TransferActionBase memeber variable.
	 * 
	 * @return TransferActionBase
	 */
	public TransferActionBase getTrActionBase() {
		Exception e = this;
		TransferActionBase retTrActionBase = this.trActionBase;

		while (e != null) {

			if (e instanceof EngineBaseException) {

				retTrActionBase = ((EngineBaseException) e).trActionBase;

				e = (Exception) ((EngineBaseException) e).getNestedException();
			} else {

				return retTrActionBase;

			}

		}
		return retTrActionBase;
	}

	/**
	 * Get the reason for error message
	 * 
	 * @return reason for error message
	 */

	public String getErrorReasonMessage() {

		StringBuffer reason = new StringBuffer("");
		Exception e = this;

		try {
			while (e != null) {

				if (reason.length() > 0) {
					reason.append("->");
				}

				if (e instanceof EngineException) {
					reason = new StringBuffer(((EngineException) e).getErrorMessage());
					e = (Exception) ((EngineException) e).getNestedException();
				} else if (e instanceof EngineMetaDataException) {
					reason = new StringBuffer(((EngineMetaDataException) e).getErrorMessage());
					e = (Exception) ((EngineMetaDataException) e).getNestedException();
				} else if (e instanceof RemoveDataException) {
					reason = new StringBuffer(((RemoveDataException) e).getErrorMessage());
					e = (Exception) ((RemoveDataException) e).getNestedException();
				} else if (e instanceof SQLException) {
					reason.append("SQLError=").append(((SQLException) e).getErrorCode());
					reason.append(", Message=").append(((SQLException) e).getMessage());

					e = null;
				} else if (e instanceof RockException) {
					reason.append("DBAccess Err:").append(e.getMessage());
					e = (Exception) ((RockException) e).getNestedException();
					if (e != null) {
						reason.append("(").append(e.getClass()).append(")");
						e = null;
					}
				} else if (e instanceof PluginException) {
					reason = new StringBuffer("Plugin error:").append(e.getMessage());
					e = (Exception) ((PluginException) e).getNestedException();

					if (e != null) {
						reason.append("(").append(e.getClass()).append(")");
						e = null;
					}
				} else {
					reason.append(e.getMessage());
					e = null;
				}
			}
		} catch (Throwable t) {
			reason = new StringBuffer(t.getMessage());
		}
		return reason.toString();
	}

}
