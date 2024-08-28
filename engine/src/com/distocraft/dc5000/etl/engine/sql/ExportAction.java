package com.distocraft.dc5000.etl.engine.sql;

import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * Common parent for all Loader classes <br>
 * <br>
 * TODO usage <br>
 * TODO used databases/tables <br>
 * <br>
 * Where-column of this action needs to a serialized properties-object which is
 * stored in class variable whereProps. ActionContents-column shall contain
 * velocity template evaluated to get load clause <br>
 * <br>
 * Copyright Distocraft 2005 <br>
 * <br>
 * $id$
 * 
 * @author lemminkainen, savinen, melantie, melkko
 */
public class ExportAction extends SQLOperation {

	private static final Logger log = Logger.getLogger("etlengine.Export");
	private final SetContext sctx;
	private final String where;

	public ExportAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final SetContext sctx)
			throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.where = trActions.getWhere_clause();
		this.sctx = sctx;

	}

	public void execute() throws EngineException {

		log.fine("Executing...");

		final RockFactory r = this.getConnection();

		try {

			final Properties prop = TransferActionBase.stringToProperties(where);

			final Class<?> c = Class.forName("com.distocraft.dc5000.etl.importexport.ETLCExport");
			final Class<?>[] parameterTypes = { Properties.class, Connection.class };

			final Object args[] = { prop, r.getConnection() };

			final Constructor<?> cont = c.getConstructor(parameterTypes);

			final Object action = cont.newInstance(args);

			final Method getViewName = c.getMethod("exportXml", String.class);

			final StringWriter sw = (StringWriter) getViewName.invoke(action, "");

			sctx.put("exportData", sw.toString());

		} catch (Exception e) {
			log.log(Level.WARNING, "Could Not Export Table(s)", e);
		} finally {

			try {
				r.getConnection().commit();
			} catch (Exception e) {
				log.log(Level.FINEST, "error committing", e);
			}

		}

	}

}
