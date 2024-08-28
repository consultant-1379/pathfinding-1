package com.distocraft.dc5000.etl.engine.system;

import static org.junit.Assert.*;

import com.ericsson.eniq.common.testutilities.DirectoryHelper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.main.TestITransferEngineRMI;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 *
 * @author ejarsok
 *
 */

public class ReloadDBLookupsActionTest {

    private static ReloadDBLookupsAction rdb;

    private static Statement stm;
  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "ReloadDBLookupsActionTest");

    @BeforeClass
  public static void init() throws Exception {
      DirectoryHelper.mkdirs(TMP_DIR);
    StaticProperties.giveProperties(new Properties());

        setUpPropertiesFileAndProperty();

        final Long collectionSetId = 1L;
        final Long transferActionId = 1L;
        final Long transferBatchId = 1L;
        final Long connectId = 1L;
        RockFactory rockFact = null;
        final SetContext sctx = new SetContext();
        final Logger clog = Logger.getLogger("Logger");

        Class.forName("org.hsqldb.jdbcDriver");

        Connection c;

        c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
        stm = c.createStatement();

        stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
                + "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

        stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

        rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);

        final Meta_versions version = new Meta_versions(rockFact);
        final Meta_collections collection = new Meta_collections(rockFact);
        final Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
        trActions.setAction_contents("tableName=tname\n");

        sctx.put("RowsAffected", 2);

        try {
            rdb = new ReloadDBLookupsAction(version, collectionSetId, collection, transferActionId, transferBatchId,
                    connectId, rockFact, trActions, sctx, clog);
        } catch (final EngineMetaDataException e1) {
            e1.printStackTrace();
        }
    }

    private static void setUpPropertiesFileAndProperty() throws IOException {
        System.setProperty(Constants.DC_CONFIG_DIR_PROPERTY_NAME, TMP_DIR.getPath());
        final File prop = new File(TMP_DIR, "ETLCServer.properties");
        prop.deleteOnExit();

        final PrintWriter pw = new PrintWriter(new FileWriter(prop));
        pw.write("name=value");
        pw.close();
    }

    @Test
    public void testExecute() throws Exception {

        final TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(false);
        rdb.execute();

        assertEquals("tname", ttRMI.getDBLookup());

    }

    @Test
    public void testExecute2() {
        try {
            final TestITransferEngineRMI ttRMI = new TestITransferEngineRMI(true);
            rdb.execute();
            fail("testExecute2() failed, should't execute this line");
        } catch (final Exception e) {

        }
    }

    @AfterClass
    public static void clean() {
      DirectoryHelper.delete(TMP_DIR);
        try {
            stm.execute("DROP TABLE Meta_collection_sets");
        } catch (final SQLException e) {
            e.printStackTrace();
        }
    }

}
