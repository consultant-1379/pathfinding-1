package ssc.rockfactory;

import java.sql.Types;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

public abstract class DataValidator {

  public static void validateData(final Object obj, final String name, final int sqlType, final int size, final int digits){

    switch (sqlType) {

    case Types.BLOB:
    case Types.BINARY:
    case Types.LONGVARBINARY:
    case Types.VARBINARY:

      final byte[] b = (byte[]) obj;

      if (b != null) {

        if (b.length > size) {
          throw new InvalidDataException(name + ": invalid datasize " + b.length + " != " + size);
        }
      }

      break;

    case Types.BIT:

      if (obj == null) {
        throw new InvalidDataException(name + ": BIT value cant be null");
      }

      break;

    case Types.CLOB:
    case Types.CHAR:
    case Types.LONGVARCHAR:
    case Types.VARCHAR:

      final String s = (String) obj;

      if (s != null) {

        if (s.length() > size) {
          throw new InvalidDataException(name + ": invalid datasize " + s.length() + " != " + size);
        }
      }

      break;

    case Types.NULL:
      // ??????????????
      break;

    case Types.DECIMAL:
    case Types.NUMERIC:
      
      
      if (obj instanceof Double){
        // double
        final Double dn = (Double) obj;

        if (dn.isNaN()) {
          throw new InvalidDataException(name + ": invalid value: " + dn);
        }

        if (dn.isInfinite()) {
          throw new InvalidDataException(name + ": infinite value: " + dn);
        }
           
        final DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(digits);
        df.setMaximumIntegerDigits(size);
        df.setGroupingUsed(false);
        final DecimalFormatSymbols dfs = new DecimalFormatSymbols();
        dfs.setDecimalSeparator('.');
        df.setDecimalFormatSymbols(dfs);
        final Double ndn = Double.parseDouble(df.format(dn));
        
        if (!dn.equals(ndn)){
          throw new InvalidDataException(name + ": invalid datasize ("+size+") or digits ("+digits+") : " + dn.toString());
        }
        
        break;
      } else {
        // long
        
        final Long l = (Long) obj;

        if (l.intValue() > 9223372036854775807l) {
          throw new InvalidDataException(name + ": value too large (9223372036854775807): " + l);
        }

        if (l.intValue() < -9223372036854775808l) {
          throw new InvalidDataException(name + ": value too small (-9223372036854775808): " + l);
        }
        
        break;
      }
      
    case Types.BIGINT:
      // long
      
      final Long l = (Long) obj;

      if (l.intValue() > 9223372036854775807l) {
        throw new InvalidDataException(name + ": value too large (9223372036854775807): " + l);
      }

      if (l.intValue() < -9223372036854775808l) {
        throw new InvalidDataException(name + ": value too small (-9223372036854775808): " + l);
      }
      
      break;

    case Types.TINYINT:
      // short
      final Short sh = (Short) obj;

      if (sh.intValue() > 255) {
        throw new InvalidDataException(name + ": value too large (>255): " + sh);
      }

      if (sh.intValue() < 0) {
        throw new InvalidDataException(name + ": value too small (<0): " + sh);
      }

      break;

    case Types.INTEGER:
      // int
      
      final Integer i = (Integer) obj;

      if (i > 2147483647) {
        throw new InvalidDataException(name + ": value too large (2147483647): " + i);
      }

      if (i < -2147483648) {
        throw new InvalidDataException(name + ": value too small (-2147483648): " + i);
      }
 
      break;

    case Types.SMALLINT:
      // int
      final Integer si = (Integer) obj;

      if (si > 32767) {
        throw new InvalidDataException(name + ": value too large (32767): " + si);
      }

      if (si < -32768) {
        throw new InvalidDataException(name + ": value too small (-32768): " + si);
      }
      
      break;

    case Types.REAL:
      // float
      final Float f = (Float) obj;

      if (f.isNaN()) {
        throw new InvalidDataException(name + ": invalid value: " + f);
      }

      if (f.isInfinite()) {
        throw new InvalidDataException(name + ": infinite value: " + f);
      }

      if (f.doubleValue() > 3.402823466e+38) {
        throw new InvalidDataException(name + ": value too large (3.402823466e+38): " + f);
      }

      if (f.doubleValue() < 1.175494351e-38) {
        throw new InvalidDataException(name + ": value too small (1.175494351e-38): " + f);
      }
      
      break;

    case Types.DOUBLE:
      // double
      final Double d = (Double) obj;

      if (d.isNaN()) {
        throw new InvalidDataException(name + ": invalid value: " + d);
      }

      if (d.isInfinite()) {
        throw new InvalidDataException(name + ": infinite value: " + d);
      }

      if (d > 1.797693134862315708e+308) {
        throw new InvalidDataException(name + ": value too large (1.797693134862315708e+308): " + d);
      }

      if (d < 2.2250738585072014e-308) {
        throw new InvalidDataException(name + ": value too small (2.2250738585072014e-308): " + d);
      }
      
      break;

    case Types.FLOAT:
      // double
      break;

    case Types.DATE:
      // Date d = (Date) obj;
      break;

    case Types.TIME:
      // Time t = (Time) obj;
      break;

    case Types.TIMESTAMP:
      // Timestamp ts = (Timestamp) obj;
      break;

    default:
      break;

    }

  }

  public static String escapeXML(final String s) {
    final StringBuffer str = new StringBuffer();
    final int len = (s != null) ? s.length() : 0; // NOPMD
    for (int i=0; i<len; i++) {
       final char ch = s.charAt(i);
       switch (ch) {
       case '<': str.append("&lt;"); break;
       case '>': str.append("&gt;"); break;
       case '&': str.append("&amp;"); break;
       case '"': str.append("&quot;"); break;
       case '\'': str.append("&apos;"); break;
       default: str.append(ch);
     }
    }
    return str.toString();
  }
  

  public static String wrap(final String name, final int sqlType){
	  return wrap(name, sqlType, false);
  }

  
  
  public static String wrap(final String name, final int sqlType, final boolean wrapNull){

    if (!wrapNull && (name==null || name.equalsIgnoreCase("null"))){
      return name;
    }
    
    switch (sqlType) {
   
    case Types.CHAR:
    case Types.LONGVARCHAR:
    case Types.VARCHAR:
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return "'"+name.replace("'", "''")+"'";
      
    case Types.BLOB:
    case Types.BINARY:
    case Types.LONGVARBINARY:
    case Types.VARBINARY:
    case Types.BIT:
    case Types.CLOB:
    case Types.NULL:
    case Types.DECIMAL:
    case Types.NUMERIC:
    case Types.BIGINT:
    case Types.TINYINT:
    case Types.INTEGER:
    case Types.SMALLINT:
    case Types.REAL:
    case Types.DOUBLE:
    case Types.FLOAT:
      return name;
      
    default:
      return name;

    }
  }

  
}
