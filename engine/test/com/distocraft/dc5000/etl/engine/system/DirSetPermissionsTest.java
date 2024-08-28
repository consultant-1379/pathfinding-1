package com.distocraft.dc5000.etl.engine.system;

import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.testutilities.UnitDatabaseTestCase;
import java.io.File;
import java.io.IOException;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ssc.rockfactory.RockFactory;

public class DirSetPermissionsTest extends UnitDatabaseTestCase {

  private static final String ETLDATA_DIR = "/eniq/data/etldata";
  private final String reldir = "dim_e_erbs_elembh_bhtype/joined/";
  private final File edir = new File(ETLDATA_DIR + "/" + reldir);
  private final int permissions = 750;
  private final String owner = "dcuser";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("ETLDATA_DIR", ETLDATA_DIR);
    setup(TestType.unit);
  }

  @Test
  public void testExecute() throws Exception {
    final Long collectionSetId = 1L;
    final Long collectionId = 2L;
    final Long transferActionId = 3L;
    final Long transferBatchId = 1L;
    final Long connectId = 1L;


    final RockFactory etlrep = getRockFactory(Schema.etlrep);
    final Meta_versions version = new Meta_versions(etlrep);
    final Meta_collections collection = new Meta_collections(etlrep);
    collection.setCollection_id( collectionId );
    final Meta_transfer_actions trActions = new Meta_transfer_actions(etlrep);

    final Statement stmt = etlrep.getConnection().createStatement();

    stmt.executeUpdate("INSERT INTO META_COLLECTION_SETS VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");

    stmt.executeUpdate("insert into META_COLLECTIONS values ("+collectionId+", 'Directory_Checker_DC_E_ERBS', null, " +
      "null, null, null, 0, 0, 0, 'N', 'N', null, '((20))', "+collectionSetId+", null, 0, 30, 'Y', 'Install', 'Y'," +
      " null, 'N', null);");

    final String dir = "${ETLDATA_DIR}/"+reldir;
    stmt.executeUpdate("insert into META_TRANSFER_ACTIONS values ('((20))', "+transferActionId+", "+collectionId+
      ", "+collectionSetId+", 'CreateDir', 'CreateDir_dim_e_erbs_elembh_bhtype_joined', 144, null, '"+dir+"', '#\n" +
      "#Mon Nov 22 19:07:21 IST 2010\n" +
      "permission="+permissions+"\n" +
      "owner="+owner+"\n" +
      "', 'Y', 2, '', '', '', '');");

    stmt.close();

    final DirSetPermissions action = new DirSetPermissions(version, collectionSetId, collection, transferActionId,
      transferBatchId, connectId, etlrep, trActions) {
      @Override
      void runProcess(final String commands) throws IOException, InterruptedException {
        checkCommands(commands);
        if(File.separatorChar == '/'){
          super.runProcess(commands);
          
        }
      }
    };

    action.execute();
  }

  private void checkCommands(final String commands) {
    Assert.assertTrue("No mkdir command found", commands.contains("mkdir -p " + edir.getPath()));
    Assert.assertTrue("No chmod command found", commands.contains("chmod "+permissions+" " + edir.getPath()));
    Assert.assertTrue("No chown command found", commands.contains("chown "+owner+" " + edir.getPath()));
    Assert.assertFalse("chgrp command found (no value so shouldn't be generated",
      commands.contains("chgrp "));
  }
}
