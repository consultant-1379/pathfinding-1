package com.ericsson.eniq.common.setWizards;

import ssc.rockfactory.RockFactory;

import com.ericsson.eniq.common.Constants;

public class EventsCreateIDirCheckerSet extends CreateIDirCheckerSet {

  public EventsCreateIDirCheckerSet(final String objType, final String version, final RockFactory rock,
      final long techPackID, final String name, final String name1,final String parsertype) throws Exception {
    super(objType, version, rock, techPackID, name, name1,parsertype);
  }

  @Override
  protected String getIBaseDirectory() {
    return Constants.ENIQ_EVENTS_TOPOLOGY_DIR;
  }

}
