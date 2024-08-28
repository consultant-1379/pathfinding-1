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

import org.jdesktop.application.ResourceMap;

import ssc.rockfactory.RockFactory;

/**
 * This class is used for creating the interface set for a ENIQ STATS techpack.
 * 
 * @author epaujor
 * 
 */
public class StatsCreateInterfaceSet extends CreateInterfaceSet {

  public StatsCreateInterfaceSet(final String objType, final String templateDir, final String tpVersion,
      final String versionID, final RockFactory dwhrepRock, final RockFactory rock, final long techPackID,
      final String interfaceName, final String interfaceVersion, final String adapterType, final String adapterName,
      final String dirName, final String connectionID, final ResourceMap resourceMap) throws SQLException {
    super(objType, templateDir, tpVersion, versionID, dwhrepRock, rock, techPackID, interfaceName, interfaceVersion,
        adapterType, adapterName, dirName, connectionID, resourceMap);
  }
}
