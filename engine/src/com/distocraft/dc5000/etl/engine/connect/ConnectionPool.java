package com.distocraft.dc5000.etl.engine.connect;

import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;

/**
 * This class stores all of the db connections used by one transfer collection
 * if a connection is already created no new one is created, instead the address
 * is returned
 * 
 */
public class ConnectionPool {

	// Created connections.
	final private List<DbConnection> connections;

	// The database connection for the metadata
	final private RockFactory rockFact;

	private static Logger log = Logger
			.getLogger("etlengine.engine.ConnectionPool");

	public ConnectionPool(final RockFactory rockFact) {
		this.rockFact = rockFact;
		this.connections = new Vector<DbConnection>();
	}

	public RockFactory getConnect(final TransferActionBase trActionBase,
			final String versionNumber, final Long connectId)
			throws EngineMetaDataException {

//		log.log(Level.FINEST, "getConnection", new Exception());
		
		for (int i = 0; i < connections.size(); i++) {

			final DbConnection conn = connections.get(i);
			if (conn.getConnectId().equals(connectId)) {
				log.finest(" retrieved connection from pool " + connectId);
				return conn.getRockFactory();
			}
		}

		final DbConnection conn = new DbConnection(trActionBase, this.rockFact,
				versionNumber, connectId);
		connections.add(conn);
		log.finest(" Connection created " + connectId);

		return conn.getRockFactory();
	}

	public int cleanPool() {
		
		log.log(Level.FINEST, "cleanPool", new Exception());		

		int count = 0;

		for (int i = 0; i < connections.size(); i++) {

			try {

				final DbConnection conn = connections.get(i);

				if (conn != null && conn.getRockFactory() != null) {
					if (!conn.getRockFactory().getConnection().isClosed()) {
						conn.getRockFactory().getConnection().close();
						log.finest("Connection closed " + conn.getConnectId());
						count++;
					}
				}

			} catch (final Exception e) {
				log.warning("Error while closing the connections in connection pool.\n"+e);
			}

		}

		log.finest("Connections cleared: " + count);
		connections.clear();

		return count;

	}

	public int count() {

		int count = 0;

		for (int i = 0; i < this.connections.size(); i++) {

			try {

				final DbConnection conn = connections.get(i);
				if (conn != null) {
					count++;
				}

			} catch (final Exception e) {
				log.warning("error while counting connection in connection pool.");

			}

		}

		return count;

	}

}