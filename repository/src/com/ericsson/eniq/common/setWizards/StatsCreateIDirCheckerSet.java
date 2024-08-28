package com.ericsson.eniq.common.setWizards;

import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;import ssc.rockfactory.RockFactory;

public class StatsCreateIDirCheckerSet extends CreateIDirCheckerSet {

  public StatsCreateIDirCheckerSet(final String objType, final String version, final RockFactory rock,
      final long techPackID, final String name, final String name1,final String parsertype) throws Exception {
    super(objType, version, rock, techPackID, name, name1,parsertype);
  }

}
