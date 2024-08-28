package com.distocraft.dc5000.etl.engine.main.engineadmincommands;

import java.util.Properties;

import org.hamcrest.Description;
import org.jmock.api.Action;
import org.jmock.api.Invocation;

/**
 * Action method for setting a property. Can be called by a test from jMock to
 * simulate setting a property.
 * 
 * @author eneacon
 * 
 * @param <T>
 */
public class SetProperty<T> implements Action {

  private String key;

  private String value;

  private Properties prop;

  public SetProperty(Properties prop, String key, String value) {
    this.prop = prop;
    this.key = key;
    this.value = value;
  }

  public Object invoke(Invocation invocation) throws Throwable {
    prop.setProperty(key, value);
    return null;
  }

  public void describeTo(Description description) {
    description.appendText("Test...");
  }
}
