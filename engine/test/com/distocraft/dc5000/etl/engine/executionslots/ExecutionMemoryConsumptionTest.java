package com.distocraft.dc5000.etl.engine.executionslots;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ExecutionMemoryConsumptionTest {

  public static final String TEST_APPLICATION = ExecutionMemoryConsumption.class.getName();

  public List<Object> list = new ArrayList<Object>();;
  
  ExecutionMemoryConsumption emc = ExecutionMemoryConsumption.instance();
  
  @Test
  public void testSize() throws Exception {
	  assertEquals(0, emc.size());
	}
  
  @Test
  public void testCalculate() throws Exception {
	  final int totalMemUsageMB = emc.calculate();
	  assertThat(totalMemUsageMB, is(0));
	  assertEquals(0, emc.size());
    }
  
  @Test
  public void testAddandRemove() throws Exception {
	  Object obj = new Object();
	  emc.add(obj);
	  assertEquals(1, emc.size());
	  emc.remove(obj);
	  assertEquals(0, emc.size());
    }

}