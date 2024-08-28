package com.ericsson.eniq.repository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class AlarmPasswordEncryption {
	public static void main(String[] args) {
		RockFactory etlRep = null;
		AlarmPasswordEncryption alarm = new AlarmPasswordEncryption();
		try {
			etlRep = alarm.getRockFactoryObj();
			final Meta_transfer_actions whereAction = new Meta_transfer_actions(etlRep);
			whereAction.setEnabled_flag("Y");
			whereAction.setAction_type("AlarmHandler");
			final Meta_transfer_actionsFactory actionsFactory = new Meta_transfer_actionsFactory(etlRep, whereAction);
			Vector<Meta_transfer_actions> alarmHandlerActions = actionsFactory.get();

			if (alarmHandlerActions.isEmpty()) {
				System.out.println("No Active AlarmHandler actions not found.");
				System.exit(1);
				return;
			}

			Iterator<Meta_transfer_actions> alarmHandlerActionsIter = alarmHandlerActions.iterator();

			while (alarmHandlerActionsIter.hasNext()) {

				Meta_transfer_actions currAction = alarmHandlerActionsIter.next();
				Properties currActionProperties = new Properties();
				String actionContents = currAction.getAction_contents();
				if (actionContents != null && actionContents.length() > 0) {
					alarm.loadProperties(currActionProperties, actionContents);
				}
				if (currActionProperties.containsKey("password")) {
					alarm.encryptpassInDB(currActionProperties, currAction);
				}
			}
		} catch (Exception e) {
			System.out.println("Exception occurred while encrypting alarm password in Meta_Trasnfer_Actions:" + e);
			e.printStackTrace();
			System.exit(3);
		} finally {
			try {
				if (etlRep != null)
					etlRep.getConnection().close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.exit(0);
	}

	private void encryptpassInDB(Properties currActionProperties, Meta_transfer_actions currAction) {
		try {
			if (!currActionProperties.containsKey("EncryptionFlag")) {
				currActionProperties.put("EncryptionFlag", "N");
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			currActionProperties.store(baos, "");
			currAction.setAction_contents(baos.toString());
			currAction.setIsDecryptionRequired(false);
			currAction.updateDB();
		} catch (SQLException | RockException e) {
			e.printStackTrace();
			System.exit(3);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(3);
		}
	}

	private void loadProperties(Properties currActionProperties, String actionContents) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(actionContents.getBytes());
			currActionProperties.load(bais);
			bais.close();
		} catch (Exception e) {
			return;
		}
		
	}

	public RockFactory getRockFactoryObj() throws Exception {
		final String confDir = System.getProperty("CONF_DIR", "/eniq/sw/conf");
		Properties props = new ETLCServerProperties(confDir + File.separator + "ETLCServer.properties");
		return new RockFactory(props.getProperty(ETLCServerProperties.DBURL),
				props.getProperty(ETLCServerProperties.DBUSERNAME), props.getProperty(ETLCServerProperties.DBPASSWORD),
				props.getProperty(ETLCServerProperties.DBDRIVERNAME), "UpdateDBPassword", false);
	}
}
