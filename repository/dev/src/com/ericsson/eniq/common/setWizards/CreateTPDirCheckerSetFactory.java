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
 * This factory class is used for creating a directory checker set based on the
 * type of the techpack.
 * 
 * @author epaujor
 * 
 */
public class CreateTPDirCheckerSetFactory {

  /**
   * Creates the specific directory checker set and returns it as a instance of
   * the superclass, CreateTPDirCheckerSet
   * 
   * @param name
   * @param version
   * @param versionid
   * @param type
   * @param dwhrepRock
   * @param rock
   * @param techPackID
   * @param techPackName
   * @return
   * @throws SQLException
   */
  public static CreateTPDirCheckerSet createTPDirCheckerSet(final String name, final String version,
      final String versionid, final String type, final RockFactory dwhrepRock, final RockFactory rock,
      final long techPackID, final String techPackName) throws SQLException {

    if (type != null && type.startsWith(Constants.ENIQ_EVENT) && techPackName.contains(Constants.SONV)) {
      return new SonvEventsCreateTPDirCheckerSet(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
    } else if (type != null && type.startsWith(Constants.ENIQ_EVENT) && techPackName.contains("EVENT_E_TERM")) {
      return new DimEventsCreateTPDirCheckerSet(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
    } else if (type != null && type.startsWith(Constants.ENIQ_EVENT)) {
      return new EventsCreateTPDirCheckerSet(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
    }

    return new StatsCreateTPDirCheckerSet(name, version, versionid, dwhrepRock, rock, techPackID, techPackName);
  }
}
