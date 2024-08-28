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

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;import com.ericsson.eniq.common.Constants;

/**
 * This factory class is used for creating the interface directory set based on the techpack name
 * 
 * @author epaujor
 * 
 */
public class CreateIDirCheckerSetFactory {

  /**
   * Creates the specific interface directory set and returns it as a instance of the superclass, CreateIDirCheckerSet
   * 
   * @param objType
   * @param version
   * @param rock
   * @param techPackID
   * @param name
   * @param name1
   * @return
   * @throws Exception
   */
  public static CreateIDirCheckerSet createIDirCheckerSet(final String objType, final String version,
      final RockFactory rock, final long techPackID, final String name, final String name1,final String parsertype) throws Exception {
    if (name != null && name.contains(Constants.DIM_E_SGEH)) {
      return new EventsCreateIDirCheckerSet(objType, version, rock, techPackID, name, name1,parsertype);
    }
    return new StatsCreateIDirCheckerSet(objType, version, rock, techPackID, name, name1,parsertype);
  }
}
