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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.main.TestITransferEngineRMI;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 *
 * @author ejarsok
 *
 */

public class ReloadTransformationsActionTest {

    private static ReloadTransformationsAction rta = null;

    private static Statement stm;


  private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "ReloadTransformationsActionTest");

    @BeforeClass
    public static void init() throws Exception {
      DirectoryHelper.mkdirs(TMP_DIR);
    StaticProperties.giveProperties(new Properties());
        setUpPropertyFileAndProperty();

        final Long collectionSetId = 1L;
        final Long transferActionId = 1L;
        final Long transferBatchId = 1L;
        final Long connectId = 1L;
        RockFactory rockFact = null;
        Connection c;

        Class.forName("org.hsqldb.jdbcDriver");

        c = DriverManager.getConnection("jdbc:hsqldb:mem:.", "SA", "");
        stm = c.createStatement();

        stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
                + "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");

        stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

        rockFact = new RockFactory("jdbc:hsqldb:mem:.", "SA", "", "org.hsqldb.jdbcDriver", "conName", true);

        final Meta_versions version = new Meta_versions(rockFact);
        final Meta_collections collection = new Meta_collections(rockFact);
        final Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);

        rta = new ReloadTransformationsAction(version, collectionSetId, collection, transferActionId, transferBatchId,
                connectId, rockFact, trActions);

    }

    private static void setUpPropertyFileAndProperty() throws IOException {
        System.setProperty(Constants.DC_CONFIG_DIR_PROPERTY_NAME, TMP_DIR.getPath());
        final File prop = new File(TMP_DIR, "ETLCServer.properties");
        prop.deleteOnExit();

        final PrintWriter pw = new PrintWriter(new FileWriter(prop));
        pw.write("name=value");
        pw.close();
    }

    @Test
    public void testExecute() throws Exception {

        new TestITransferEngineRMI(false);
        rta.execute();

    }

    @Test
    public void testExecute2() {
        try {
            new TestITransferEngineRMI(true);
            rta.execute();
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
