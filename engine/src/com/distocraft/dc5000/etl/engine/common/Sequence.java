/*
 * Created on 24.8.2004
 *
 */
package com.distocraft.dc5000.etl.engine.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author savinen 
 */
public class Sequence {

	private long curID;
	private long resID;
	static final int IDPOOLSIZE = 50;
	// TODO get IDPOOL from property
	private static Sequence singletonSequence = null;

	/**
	 * At first call a Sequence is created. After fist call the same sequence is
	 * returned.
	 * 
	 * @return
	 */
	public synchronized static Sequence instance() throws IOException {
		if (singletonSequence == null) {
			singletonSequence = new Sequence();
		}

		return singletonSequence;
	}

	/**
	 * Constructor
	 * 
	 */
	private Sequence() throws IOException {
		this.reserveIDs();
	}

	/**
	 * 
	 * Reserves number of ID from the ID file.
	 * 
	 * 
	 * @throws Exception
	 */
	private void reserveIDs() throws IOException {

		try {
			// TODO get ID filename from property
			final BufferedReader br = new BufferedReader(new FileReader("IDs.info"));
			final String value = br.readLine();
			this.curID = Long.parseLong(value);
			this.resID = curID + IDPOOLSIZE;
			br.close();

		} catch (Exception e) {
			this.resID = IDPOOLSIZE;
			this.curID = 0;
		}

		try {
			final BufferedWriter bw = new BufferedWriter(new FileWriter("IDs.info"));
			bw.write(Long.toString(this.resID));
			bw.close();

		} catch (IOException e) {
			throw new IOException("Cannot write ID file ", e);
		}

	}

	/**
	 * returns a new unique ID.
	 * 
	 * @return ID
	 */
	public synchronized long getNewId() throws IOException {

		if (this.curID < this.resID) {
			this.curID++;
		} else {
			this.reserveIDs();
			this.curID++;
		}

		return this.curID;

	}

}
