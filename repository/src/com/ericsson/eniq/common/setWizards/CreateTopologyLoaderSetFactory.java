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
 * This factory class is used for creating a topology loader set based on the techpack name
 * 
 * @author epaujor
 * 
 */
public class CreateTopologyLoaderSetFactory {

  /**
   * Creates the specific topology loader set and returns it as a instance of the superclass, CreateTopologyLoaderSet
   * 
   * @param templateDir
   * @param name
   * @param version
   * @param versionid
   * @param dwhrepRock
   * @param rock
   * @param techPackID
   * @param techPackName
   * @param schedulings
   * @return
   * @throws Exception
   */
  public static CreateTopologyLoaderSet createTopologyLoaderSet(final String templateDir, final String name,
      final String version, final String versionid, final RockFactory dwhrepRock, final RockFactory rock,
      final int techPackID, final String techPackName, final boolean schedulings) throws SQLException {

    if (techPackName != null && techPackName.startsWith(Constants.DIM_E_SGEH)) {
      return new EventsCreateTopologyLoaderSet(templateDir, name, version, versionid, dwhrepRock, rock, techPackID,
          techPackName, schedulings);
    }
    return new StatsCreateTopologyLoaderSet(templateDir, name, version, versionid, dwhrepRock, rock, techPackID,
        techPackName, schedulings);
  }
}
