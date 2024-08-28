package com.distocraft.dc5000.etl.engine.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ColumnFormatTest {

  @Test
  public void test() {
    final Map<String, String> header = new LinkedHashMap<String, String>();
    header.put("x", "Col1");
    header.put("y", "Col2");
    header.put("z", "Col3");
    header.put("i", "Col4");
    header.put("j", "Col5");
    header.put("k", "Col6");

    final List<Map<String, String>> data = new ArrayList<Map<String, String>>();
    final Map<String, String> row1 = new HashMap<String, String>();
    row1.put("x", "2312312321");
    row1.put("y", "1221");
    row1.put("z", "2131");
    row1.put("i", "12312321");
    row1.put("j", "123");
    row1.put("k", "1");
    data.add(row1);

    final String expected = "+----------+----+----+--------+----+----+\n" +
      "|Col1      |Col2|Col3|Col4    |Col5|Col6|\n" +
      "+----------+----+----+--------+----+----+\n" +
      "|2312312321|1221|2131|12312321|123 |1   |\n" +
      "+----------+----+----+--------+----+----+";

    if (!expected.equals(ColumnFormat.format(header, data))) {
      fail("Return format was unexpected");
    }

  }

  @Test
  public void testNullValue() {
    final Map<String, String> header = new LinkedHashMap<String, String>();
    header.put("x", "C1");
    header.put("y", "C2");

    final List<Map<String, String>> data = new ArrayList<Map<String, String>>();
    final Map<String, String> row1 = new HashMap<String, String>();
    row1.put("x", "c11");
    row1.put("y", null);
    data.add(row1);

    final String expectedString =
      "+---+--+\n" +
      "|C1 |C2|\n" +
      "+---+--+\n" +
      "|c11|  |\n" +
      "+---+--+";

    try{
      final String generatedString = ColumnFormat.format(header, data);
      assertEquals("Generated table not correct", expectedString, generatedString);
    } catch (NullPointerException e){
      fail("Null values in data not being handled correctly!");
    }

  }
}
