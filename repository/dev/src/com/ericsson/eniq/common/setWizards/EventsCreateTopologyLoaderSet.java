/**------------------------------------------------------------------------
 *
 *
 *      COPYRIGHT (C)                   ERICSSON RADIO SYSTEMS AB, Sweden
 *
 *      The  copyright  to  the document(s) herein  is  the property of
 *      Ericsson Radio Systems AB, Sweden.
 *
 *      The document(s) may be used  and/or copied only with the written
 *      permission from Ericsson Radio Systems AB  or in accordance with
 *      the terms  and conditions  stipulated in the  agreement/contract
 *      under which the document(s) have been supplied.
 *
 *------------------------------------------------------------------------
 */
package com.ericsson.eniq.common.setWizards;

import java.sql.SQLException;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;

/**
 * This class is used for creating the topology loader set for a ENIQ EVENTS techpack.
 * 
 * @author epaujor
 * 
 */
public class EventsCreateTopologyLoaderSet extends CreateTopologyLoaderSet {

  public EventsCreateTopologyLoaderSet(final String templateDir, final String name, final String version,
      final String versionid, final RockFactory dwhrepRock, final RockFactory rock, final int techPackID,
      final String techPackName, final boolean schedulings) throws SQLException {
    super(templateDir, name, version, versionid, dwhrepRock, rock, techPackID, techPackName, schedulings);
  }

  @Override
  protected String getTopologyDirectory(final String referenceTableName) {
    return Constants.EVENTS_ETLDATA_DIR + this.pathSeparator + "00" + this.pathSeparator + referenceTableName
        + this.pathSeparator + "raw" + this.pathSeparator;
  }

}
