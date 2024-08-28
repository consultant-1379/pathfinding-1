package com.ericsson.eniq.common.setWizards;

import org.apache.velocity.VelocityContext;

/**
 * @author epiituo
 * 
 */
public interface SetCreator {

  /**
   * @param skip
   * @throws Exception
   */
  public void create(boolean skip) throws Exception;

  /**
   * @throws Exception
   */
  public void removeSets() throws Exception;

  /**
   * @param templateName
   * @param context
   * @return
   * @throws Exception
   */
  public String merge(String templateName, VelocityContext context) throws Exception;

}
