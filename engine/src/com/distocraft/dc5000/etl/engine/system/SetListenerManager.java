package com.distocraft.dc5000.etl.engine.system;

import java.util.HashMap;
import java.util.Map;

/**
 * @author epiituo
 */
public class SetListenerManager {

	private static SetListenerManager instance;

	private static long nextKey = 0;

	private final Map<Long, SetListener> setListeners;

	/**
	 * Returns the singleton instance of SetListenerManager.
	 * 
	 * @return
	 */
	public static SetListenerManager instance() {
		if (instance != null) {
			return instance;
		} else {
			instance = new SetListenerManager();
			return instance;
		}
	}

	/**
	 * A private default constructor. SetListenerManager instances are created
	 * with the static method SetListenerManager.instance().
	 */
	private SetListenerManager() {
		this.setListeners = new HashMap<Long, SetListener>();
	}

	/**
	 * Stores the listener to the listener manager, and returns an id number that
	 * can be used to access the stored listener.
	 * 
	 * @param listener
	 * @return
	 */
	public long addListener(final SetListener listener) {
		final long listenerId = nextKey++;
		this.setListeners.put(listenerId, listener);
		return listenerId;
	}

	/**
	 * Returns the SetListener stored with the listenerId.
	 * 
	 * @param listenerId
	 * @return A SetListener instance stored with the listenerId. If no
	 *         SetListener can be found with the listenerId, the method returns
	 *         null.
	 */
	public SetListener get(final long listenerId) {
		return this.setListeners.get(listenerId);
	}

	/**
	 * Removes the set listener stored with the listenerId.
	 * 
	 * @param listenerId
	 */
	public void remove(final long listenerId) {
		this.setListeners.remove(listenerId);
	}

}
