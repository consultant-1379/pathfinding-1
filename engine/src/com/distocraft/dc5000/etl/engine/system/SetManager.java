/**
 * This class handles running the sets with listeners.
 */
package com.distocraft.dc5000.etl.engine.system;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.distocraft.dc5000.etl.engine.main.EngineThread;
import com.distocraft.dc5000.etl.engine.main.TransferEngine;

/**
 * @author epetrmi
 */
public class SetManager {

	private static SetManager instance;
	private static final String ID_DELIMETER = "###";
	private final Map<String, SetListener> setListeners = new HashMap<String, SetListener>();

	/**
	 * Returns the instance of SetListenerManager (singleton).
	 * 
	 * @return
	 */
	public static SetManager getInstance() {
		if (instance == null) {
			instance = new SetManager();
		}
		return instance;
	}

	public SetStatusTO executeSet(final String colSetName, final String setName, final Properties props,
			final EngineThread et, final TransferEngine transferEngine) {

		SetStatusTO ret = null;
		// Check if thread is already running
		final String setIdentifier = colSetName + ID_DELIMETER + setName;

		// We want to allow only one set execution per time
		// String setIdentifier = ID_DELIMETER;

		// Prevent concurrent running
		final SetListener alreadyRunningSetListener = getRunningSetIfExists();

		final SetListener existingListener = setListeners.get(setIdentifier);
		final String SET_IS_RUNNING = "";
		if (existingListener == null || SET_IS_RUNNING.equals(existingListener.getStatus())
				|| alreadyRunningSetListener == null) {
			// Set is not active so we can run a set in a thread

			final SetListener setListener = new SetListener();
			setListeners.put(setIdentifier, setListener);
			et.setListener = setListener;// ##TODO## Consider adding getter/setter in
																		// EngThread for this

			// We have to run this stuff in a separate thread
			final SetExecuterRunnable runnable = new SetExecuterRunnable(props, setListener, et, transferEngine);
			final Thread t = new Thread(runnable);
			t.start();

			// Get listenermanager SetStatusTO and return it!
			final SetStatusTO statusTO = setListeners.get(setIdentifier).getStatusAsTO();
			ret = statusTO;
		} else {
			// Thread is still running
			ret = setListeners.get(setIdentifier).getStatusAsTO();// ##TODO## Check
																														// that status is
																														// running
		}
		return ret;
	}

	/**
	 * Traverse through the map that holds the setListeners
	 * 
	 * @return - SetT immediately if setListener status="" (NOT_FINISHED).
	 *         Otherwise false.
	 */
	private SetListener getRunningSetIfExists() {
		for (String key : this.setListeners.keySet()) {
			final SetListener sl = this.setListeners.get(key);
			if (sl != null) {
				if (SetListener.NOTFINISHED.equals(sl.getStatus())) {
					return sl;
				}
			}
		}
		return null;
	}

	public SetStatusTO getSetStatus(final String colSetName, final String setName, final int beginIndex, final int count) {
			final String setIdentifier = colSetName + ID_DELIMETER + setName;
		// String setIdentifier = ID_DELIMETER;
		final SetListener sl = setListeners.get(setIdentifier);
		if (sl != null) {
			return sl.getStatusAsTO();
		} else {
			return null;
		}
	}
}
