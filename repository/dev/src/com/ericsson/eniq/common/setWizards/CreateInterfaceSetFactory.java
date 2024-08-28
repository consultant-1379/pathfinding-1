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

import com.ericsson.eniq.common.Constants;

/**
 * This factory class is used for creating a interface set based on the interface name
 * 
 * @author epaujor
 * 
 */
public class CreateInterfaceSetFactory {

  /**
   * Creates the specific interface set and returns it as a interface of the superclass, CreateInterfaceSet
   * 
   * @param objType
   * @param templateDir
   * @param tpVersion
   * @param versionID
   * @param dwhrepRock
   * @param rock
   * @param techPackID
   * @param interfaceName
   * @param interfaceVersion
   * @param adapterType
   * @param adapterName
   * @param dirName
   * @param connectionID
   * @return
   * @throws SQLException
   */
  public static CreateInterfaceSet createInterfaceSet(final String objType, final String templateDir,
      final String tpVersion, final String versionID, final RockFactory dwhrepRock, final RockFactory rock,
      final long techPackID, final String interfaceName, final String interfaceVersion, final String adapterType,
      final String adapterName, final String dirName, final String connectionID, final ResourceMap resourceMap)
      throws SQLException {

    if (interfaceName != null && interfaceName.contains(Constants.DIM_E_SGEH)) {
      return new EventsCreateInterfaceSet(objType, templateDir, tpVersion, versionID, dwhrepRock, rock, techPackID,
          interfaceName, interfaceVersion, adapterType, adapterName, dirName, connectionID, resourceMap);
    }
    return new StatsCreateInterfaceSet(objType, templateDir, tpVersion, versionID, dwhrepRock, rock, techPackID,
        interfaceName, interfaceVersion, adapterType, adapterName, dirName, connectionID, resourceMap);
  }
}
