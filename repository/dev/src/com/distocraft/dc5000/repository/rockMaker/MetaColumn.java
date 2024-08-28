/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/repository/java/com/distocraft/dc5000/repository/rockMaker/MetaColumn.java,v 1.2.2.2 2008/03/27 13:12:49 ejarsav Exp $
 *
 *
 * $Author: ejarsav $ $Date: 2008/03/27 13:12:49 $
 *
 * $Log: MetaColumn.java,v $
 * Revision 1.2.2.2  2008/03/27 13:12:49  ejarsav
 * userAccount added
 *
 * Revision 1.2.2.1  2008/01/22 10:43:17  ejarsav
 * R6 changes
 *
 * Revision 1.2  2006/04/20 08:14:53  savinen
 * timestamp type added
 *
 * Revision 1.1  2006/04/03 10:33:21  savinen
 * rockmaker and dwhrep metadata added
 *
 * Revision 1.4  2003/09/10 12:02:16  tommi
 * - warning displayed when same column is imported
 *   (does not throw exception anymore)
 *
 * Revision 1.3  2003/05/16 21:32:15  tommi
 * - added ordinalNumber
 *
 * Revision 1.2  2003/05/14 17:14:35  tommi
 * - package and imports updated from com.sensorsc to com.solita
 *
 * Revision 1.1  2003/05/14 15:26:42  tommi
 * - initial for rockengine-2.2
 *
 * Revision 1.12  2003/04/30 09:16:34  tommi
 * - bugfix:LONGVARCHAR was converted to Long
 * - Now uses JaggerProperties instead of System properties to get
 *   mapping values
 *
 * Revision 1.11  2003/04/24 12:41:20  tommi
 * - method split(String, String): String[] moved to Util
 *
 * Revision 1.10  2003/04/15 09:27:18  tommi
 * - Oracle specific bugfix:
 *   in method getClassType(int, String) The returned class type was incorrect
 *   The Oracle driver returns false sql type numbers
 *
 * Revision 1.9  2003/04/11 12:08:13  tommi
 * - added method: getNumbericJavaTypes(): Set
 * - added method: isNumericClassType(): boolean
 *
 * Revision 1.8  2003/03/21 14:12:11  tommi
 * - added method split(...)
 * - added method overrideClassType(...) for overriding db default data types
 *   for daos and datas
 *
 * Revision 1.7  2003/03/06 11:00:12  tommi
 * no message
 *
 * Revision 1.6  2002/05/20 07:53:46  komu
 * Fixed stupid bug.
 *
 * Revision 1.5  2002/05/20 07:26:28  komu
 * Added magic overrides.
 *
 * Revision 1.4  2002/05/15 05:30:11  komu
 * no message
 *
 * Revision 1.3  2002/05/14 14:10:31  komu
 * no message
 *
 * Revision 1.2  2002/03/22 12:28:19  komu
 * Implement Comparable.
 *
 * Revision 1.1  2002/03/01 12:07:00  komu
 * Moved.
 *
 *
 */

package com.distocraft.dc5000.repository.rockMaker;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;



/**
 * Contains meta-data for a single column of database.
 * <p>
 * Columns are immutable after they have been constructed
 * by <code>MetaDataProvider</code> and only one instance
 * is created for each column in the database.
 * <p>
 * Original Author:     Juha Komulainen
 * @author              $Author: ejarsav $
 * @since               JDK 1.2
 */
public final class MetaColumn implements Comparable {

    /** */
    private static final Set numericTypes = getNumbericJavaTypes();

    /** Name of the column */
    private final String name;

    /** Table containing this column */
    private final MetaTable table;

    /** SQL type from java.sql.Types */
    private final short sqlType;

    /** Data source dependent type name */
    private final String type;

    /** Class-name derived from sqlType and type */
    private final String classType;

    /** Name of primitive type derived from classType */
    private final String primitiveType;

    /** Default value of the column. May be null. */
    private final String defaultValue;

    /** Sequence for this column. May be null. */
    private final MetaSequence sequence;

    /** Is this column nullable? If it is not certain, we assume it is. */
    private final boolean nullable;

    /** Is this column primary key of it's table? */
    private final boolean isPrimaryKey;

    /** The number of fractional digits */
    private final int decimalDigits;

    /** Size of the column */
    private final int columnSize;

    /** Column remarks. May be null. */
    private final String remarks;

    /** The column ordinal number in db table */
    private final Integer ordinalNumber;

    /** Column where this is imported from or null */
    private MetaColumn importColumn = null;

    /** List of columns where this is columns is exported */
    private ArrayList exportColumns = null;

    /**
     * Constructs new <code>MetaColumn</code> with given arguments.
     * <p>
     * This is package private since only <code>MetaDataProvider</code>
     * should construct <code>MetaColumn</code>s.
     *
     * @param     table Table containing this column
     * @param     name Name of this column
     * @param     sqlType Type of this column as defined in java.sql.Types
     * @param     type Data source dependent type name
     * @param     defaultValue Default value of the column. May be null.
     * @param     nullable Does this column allow null values? If unknown,
     *                     use true.
     * @param     isPrimaryKey Is this column primary key of it's table
     * @param     decimalDigits The number of fractional digits
     * @param     columnSize Size of the column
     * @param     sequence Sequence to use for this column. May be null.
     * @param     remarks Column remarks. May be null.
     * @param     ordinalNumber The column ordinal number in db table
     * @throws IllegalArgumentException if some of the arguments is illegal
     */
    MetaColumn(MetaTable table,
               String name,
               short sqlType,
               String type,
               String defaultValue,
               boolean nullable,
               boolean isPrimaryKey,
               int decimalDigits,
               int columnSize,
               MetaSequence sequence,
               String remarks,
               int ordinalNumber) {

        // Check the validity of the arguments
        if (table == null)   throw new IllegalArgumentException("null table");
        if (type == null)    throw new IllegalArgumentException("null type");
        if (name == null)    throw new IllegalArgumentException("null name");
        if (name.equals("")) throw new IllegalArgumentException("empty name");

        this.table = table;
        this.name = name;
        this.sqlType = sqlType;
        this.defaultValue = defaultValue;
        this.type = type;
        this.nullable = nullable;
        this.isPrimaryKey = isPrimaryKey;
        this.decimalDigits = decimalDigits;
        this.columnSize = columnSize;
        this.sequence = sequence;
        this.remarks = remarks;
        this.ordinalNumber = new Integer(ordinalNumber);

        this.classType = overrideClassType(this.sqlType, this.type, this.decimalDigits);

        this.primitiveType = getPrimitiveType(this.classType);
    }

    /**
     * Returns the name of this column.
     *
     * @return    The name of this column
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the name of this column where first character is
     * upper-case and rest are lower-cased.
     *
     * @return    Name of this column capitalized
     */
    public String getCapitalizedName() {
        // We ensured in constructor that 'name' is at least 1 char in length.
        return Character.toUpperCase(name.charAt(0))
               + name.substring(1).toLowerCase();
    }

    /**
     * Returns the table of this column.
     *
     * @return    The table where this column belongs to
     */
    public MetaTable getTable() {
        return table;
    }

    /**
     * Returns the default-value for this column. (Possibly null.)
     * Note that the default-value might also come from a sequence.
     *
     * @return    Default-value of the column
     * @see       #getSequence()
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Returns the sequence for this column. (Possibly null.)
     * Note that if the sequence is not set, default value
     * might be defined otherwise.
     *
     * @return   Sequence for the column or null
     * @see      #getDefaultValue()
     */
    public MetaSequence getSequence() {
        return sequence;
    }

    /**
     * Returns the number of fractional digits for this column.
     * This is meaningless for columns that don't contain decimal data.
     *
     * @return    Number of fractional digits for this column
     */
    public int getDecimalDigits() {
        return decimalDigits;
    }

    /**
     * Returns the size of this column.
     *
     * @return    Size of this column
     */
    public int getColumnSize() {
        return columnSize;
    }

    /**
     * Returns the ordinal number of this column in db table
     *
     * @return The ordinal number
     */
    public Integer getOrdinalNumber() {
        return this.ordinalNumber;
    }

    /**
     * Is this column a primary key for this table?
     *
     * @return    True if this columns is a primary key of it's table
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     * Is this column imported from other table?
     *
     * @return    True if this column is imported from another table
     */
    public boolean isImported() {
        return importColumn != null;
    }

    /**
     * Does this column allow null values?
     *
     * @return    True if this column allows null-values
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Returns the column from where this column is imported or null
     * if this column is not imported.
     *
     * @return    Column from where this column is imported or null
     */
    public MetaColumn getImportColumn() {
        return importColumn;
    }

    /**
     * Returns the table from where this column is imported or null
     * if this column is not imported. Convenience method.
     *
     * @return    Table from where this column is imported or null
     */
    public MetaTable getImportTable() {
        if (importColumn != null) {
            return importColumn.getTable();
        } else {
            return null;
        }
    }

    /**
     * Is this columns exported to other tables?
     *
     * @return    True if this column is exported
     */
    public boolean isExported() {
         return exportColumns != null && exportColumns.size() != 0;
    }

    /**
     * Returns the columns in other tables that import this column.
     *
     * @return    Array of columns that import this column
     */
    public MetaColumn[] getExportColumns() {
        if (exportColumns != null) {
            MetaColumn[] array = new MetaColumn[exportColumns.size()];
            return (MetaColumn[])exportColumns.toArray(array);
        } else {
            return new MetaColumn[0];
        }
    }

    /**
     * Returns the type of this column as SQL type from java.sql.Types
     *
     * @return    SQL-type of this column
     */
    public short getSQLType() {
        return sqlType;
    }

    /**
     * Is the column a blob?
     *
     * @return    True iff column is a blob
     */
    public boolean isBlob() {
        return "Blob".equals(classType);
    }

    /**
     * Is the column a clob?
     *
     * @return    True iff column is a clob
     */
    public boolean isClob(){
        return "Clob".equals(classType);
    }

    /**
     * Is the columns either a Blob or a Clob?
     *
     * @return   True iff column is blob or clob
     */
    public boolean isLob() {
        return isBlob() || isClob();
    }

    /**
     * Get the remarks for the column.
     *
     * @return    Remarks for the column
     */
    public String getRemarks() {
        return remarks;
    }

    /**
     * Returns a String-representation of this object.
     *
     * @return    String-representation of this object
     */
    public String toString() {
        return getCapitalizedName();
    }

    /**
     * Returns the name of the Java class-type of this column.
     *
     * @return    Name of the Java class-type of this column
     */
    public String getClassType() {
        return classType;
    }

    /**
     * Returns the "user class-type" of column. Normally the same
     * as class-type, but for String is returned for Clobs and
     * byte-array for Blobs.
     */
    public String getUserClassType() {
        if (classType.equals("Blob")) {
            return "byte[]";
        } else if (classType.equals("Clob")) {
            return "String";
        } else if (classType.equals("BigDecimal")) {
            return "Long";
        }else {
            return classType;
        }
    }
    
    
    /**

     */
    public String getNewUserClassType() {

      if (numericTypes.contains(classType)) {
        return "new " + classType + " (0)";
      } else {
        if (classType.equalsIgnoreCase("Date")) {
          return "new " + classType + " (0)";
        }
        if (classType.equalsIgnoreCase("Timestamp")) {
          return "new " + classType + " (0)";
        } else if("String".equalsIgnoreCase(classType)){
          return "\"\"";
        } else
          return "new " + classType + " (\"\")";
      }
    }
    

    /**
     * Returns the primitive type for this column. If the column
     * has no corresponding primitive type, return the class instead.
     *
     * @return    Name of the primitive type for this column
     */
    public String getPrimitiveTypeName() {
        return primitiveType;
    }

    /**
     * Compares this column to another column. Ordering is
     * the ordering of the names.
     *
     * @param  obj Object to compare this object against
     * @return a negative integer, zero, or a positive integer as this
     *         column's name is less than, equal to, or greater than
     *         the specified columns's name.
     */
    public int compareTo(Object obj) {
        MetaColumn column = (MetaColumn)obj;
        return name.compareTo(column.name);
    }

    /**
     *
     * @return
     */
    public boolean isNumericClassType() {
        return numericTypes.contains(classType);
    }

    /**
     * Set the column where this column is imported from. Also adds
     * this column as an exported column to <code>importColumn</code>.
     * <p>
     * This method is package-private and should only be called
     * by <code>MetaDataProvider</code> when constructing columns.
     * <p>
     * This method may only be called once.
     *
     * @param  importColumn Column from where this column is imported
     * @throws IllegalArgumentException if importColumn is null
     * @throws IllegalStateException if import-column is already set
     */
    void setImportFromColumn(MetaColumn importColumn) {
        if (this.importColumn != null) {
            System.err.println("warning: imported column is replaced.");
            System.err.println("    this column: "+ this.table.getName() +"."+this.name);
            System.err.print("    ex-column  : " + this.importColumn.getTable());
            System.err.println("  ." + this.importColumn.name);
            System.err.print("    new-column : " + importColumn.getTable());
            System.err.println("  ." + importColumn.name);
            System.err.println();
            //throw new IllegalStateException("importColumn already set");
        }
        if (importColumn == null) {
            throw new IllegalArgumentException("null importColumn");
        }

        // Set importColumn of this column.
        this.importColumn = importColumn;

        // Now add this column as an export to the other table.
        if (importColumn.exportColumns == null) {
            importColumn.exportColumns = new ArrayList();
        }
        importColumn.exportColumns.add(this);
    }

    /**
     * Gets the name of Java-class for given SQL-type.
     *
     * @param     sqlType SQL-type of the column
     * @param     type Data-source dependent type-name of the column
     * @return    Class-type for given type
     */
    private static String getClassType(int sqlType, String type) {
        switch (sqlType) {
        case Types.BINARY:              return "byte []";
        case Types.LONGVARBINARY:       return "byte []";
        case Types.VARBINARY:           return "byte []";
        case Types.BIT:                 return "Boolean"; // String
        case Types.BLOB:                return "Blob";
        case Types.CLOB:                return "Clob";
        case Types.CHAR:                return "String";
        case Types.LONGVARCHAR:
            return "String";
        case Types.NULL:                return "String";
        case Types.VARCHAR:             return "String";
        case Types.DECIMAL:             return "Double"; // Integer / Long / Float
        case Types.NUMERIC:             return "Long";  // Double
        case Types.TINYINT:             return "Short";
        case Types.INTEGER:             return "Integer";
        case Types.SMALLINT:            return "Integer";  // Short
        case Types.BIGINT:              return "Long";
        case Types.REAL:                return "Float";
        case Types.DOUBLE:              return "Double";
        case Types.FLOAT:               return "Double";   // Float
        case Types.DATE:                return "Date";
        case Types.TIME:                return "Time";
        case Types.TIMESTAMP:           return "Timestamp";
        case Types.OTHER:
            if (type.equalsIgnoreCase("BLOB")) {
                return "Blob";
            } else if (type.equalsIgnoreCase("CLOB")) {
                return "Clob";
            } else {
                return toClassName(type);
            }
        }
        return "String";
    }

    /**
     * Returns corresponding primitive type for given class.
     * If the class does not have an associated primitive-type,
     * return the class instead.
     *
     * @param     type The class-type for which to get primitive-type
     * @return    Primitive type corresponding given class
     */
    private static String getPrimitiveType(String type) {
        if (type.equals("Integer")) {
            return "int";
        } else if (type.equals("Float")) {
            return "float";
        } else if (type.equals("Double")) {
            return "double";
        } else if (type.equals("Short")) {
            return "short";
        } else if (type.equals("Long")) {
            return "long";
        } else if (type.equals("Character")) {
            return "char";
        } else if (type.equals("Boolean")) {
            return "boolean";
        } else {
            return type;
        }
    }

    /**
     *
     * @param sqlType
     * @param type
     * @param decimalDigits
     * @return
     */
    private String overrideClassType(short sqlType, String type,
                int decimalDigits) {

/*        String classType;
        // Check if we have a magic property for this column.
        // If we don't have a type for this column, maybe we have
        // a global override for the type of this column?
        String key = "jagger.type." + table.getName() + "." + name;

        if (JaggerProperties.INSTANCE.contains(key)) {
            classType = JaggerProperties.INSTANCE.getProperty(key);
        } else {
            classType = getClassType(sqlType, type);
            String globalKey = "jagger.type." + classType;
            String override = JaggerProperties.INSTANCE.getProperty(globalKey);
            if (override != null) {
                // Check whether the syntax is jagger.type.BigDecimal=Long:0,Double
                String[] types = Util.split(override, ",");

                Map overrideTypes = new HashMap(5);
                for (int i = 0; i < types.length; i++) {
                    String[] digits = Util.split(types[i], ":");
                    if (digits.length == 2) {
                        overrideTypes.put(digits[1], digits[0]);
                    } else {
                        overrideTypes.put(null, digits[0]);
                    }
                }
                override = (String)overrideTypes.get(String.valueOf(decimalDigits));
                // If null use the default value
                if (override == null) {
                    override = (String)overrideTypes.get(null);
                }

                classType = override;
            }
        }

        return classType; */
    	
    	return  getClassType(sqlType, type);
    }

    /**
     * Collects the types that are numeric formats
     *
     * @return Class types as "java.lang.Type", but without package
     */
    private static Set getNumbericJavaTypes() {

        Set types = new HashSet();
        types.add("Long");
        types.add("Integer");
        types.add("Short");
        types.add("BigDecimal");
        types.add("Double");
        types.add("Float");
        return types;
    }

    /**
     * Creates class name: first char at uppercase, rest of the characters
     * at lowercase
     *
     * @param type The type name
     * @return New class name
     */
    private static String toClassName(String type) {
        return type.substring(0, 1).toUpperCase().concat(type.substring(1).toLowerCase());
    }
}

