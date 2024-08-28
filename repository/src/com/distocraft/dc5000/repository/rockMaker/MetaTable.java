/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/repository/java/com/distocraft/dc5000/repository/rockMaker/MetaTable.java,v 1.1 2006/04/03 10:33:21 savinen Exp $
 *
 *
 * $Author: savinen $ $Date: 2006/04/03 10:33:21 $
 *
 * $Log: MetaTable.java,v $
 * Revision 1.1  2006/04/03 10:33:21  savinen
 * rockmaker and dwhrep metadata added
 *
 * Revision 1.2  2003/05/14 17:14:35  tommi
 * - package and imports updated from com.sensorsc to com.solita
 *
 * Revision 1.1  2003/05/14 15:26:42  tommi
 * - initial for rockengine-2.2
 *
 * Revision 1.3  2002/03/22 12:28:01  komu
 * Implement Comparable.
 *
 * Revision 1.2  2002/03/07 09:22:08  komu
 * Added hasLobColumns(). Fixed bug in getLobColumns().
 *
 * Revision 1.1  2002/03/01 12:07:00  komu
 * Moved.
 *
 *
 */

package com.distocraft.dc5000.repository.rockMaker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 *
 * Contains meta-data for a single table of database.
 * <p>
 * Tables are immutable after they have been constructed
 * by <code>MetaDataProvider</code> and only one instance
 * is created for each table in the database.
 * <p>
 * Original Author:     Juha Komulainen
 * @author              $Author: savinen $
 * @since               JDK 1.2
 */
public final class MetaTable implements Comparable {

    /** Name of the table */
    private final String name;

    /** Catalog of the table. May be null. */
    private final String catalog;

    /** Schema of the table. May be null. */
    private final String schema;

    /** Type of the table */
    private final String type;

    /** Remarks for the table */
    private final String remarks;

    /** Columns of this table */
    private MetaColumn[] columns = null;

    /**
     * Construct new <code>MetaTable</code> with given arguments.
     * <p>
     * This is package private since only <code>MetaDataProvider</code>
     * should construct <code>MetaTable</code>s.
     *
     * @param     name Name of the table
     * @param     catalog Table catalog (may be null)
     * @param     schema Table schema (may be null)
     * @param     type Table type. Typical types are "TABLE", "VIEW",
     *            "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
     *            "ALIAS", "SYNONYM".
     * @param     remarks Remarks for the table (may be null)
     * @exception IllegalArgumentException if some of the arguments is illegal
     */
    MetaTable(String name, String catalog, String schema,
              String type, String remarks) {
        // Check the validity of the arguments
        if (name == null)    throw new IllegalArgumentException("null name");
        if (name.equals("")) throw new IllegalArgumentException("empty name");
        if (type == null)    throw new IllegalArgumentException("null type");

        this.name = name;
        this.catalog = catalog;
        this.schema = schema;
        this.type = type;
        this.remarks = remarks;
    }

    /**
     * Returns the name of this table.
     *
     * @return    Name of this table
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the name of this table where first character is
     * upper-case and rest are lower-cased.
     *
     * @return    Name of this table capitalized
     */
    public String getCapitalizedName() {
        return Character.toUpperCase(name.charAt(0))
               + name.substring(1).toLowerCase();
    }

    /**
     * Returns the schema for this table. May be null.
     *
     * @return    Schema of this table
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Returns the catalog for this table. May be null.
     *
     * @return    Catalog of this table
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Returns the type of the table. Typical types are "TABLE", "VIEW",
     * "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS",
     * "SYNONYM".
     *
     * @return    Type of the table
     */
    public String getType() {
        return type;
    }

    /**
     * Returns the remarks for the table. May be null, if remarks
     * are not defined.
     *
     * @return    Remarks for the table
     */
    public String getRemarks() {
        return remarks;
    }

    /**
     * Returns the columns of this table.
     *
     * @return    Columns of the table
     */
    public MetaColumn[] getColumns() {
        return (MetaColumn[])columns.clone();
    }

    /**
     * Returns the columns that form the primary key of this table.
     *
     * @return    Primary-key columns of this table
     */
    public MetaColumn[] getPrimaryKeyColumns() {
        ArrayList result = new ArrayList(columns.length);

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isPrimaryKey()) {
                result.add(columns[i]);
            }
        }

        return getColumns(result);
    }

    /**
     * Returns the column for given name or null if column with
     * given name does not exist. Search is case-insensitive.
     *
     * @param     name of the column to search
     * @return    Column with given name or null
     * @exception NullPointerException if name is null
     */
    public MetaColumn getColumn(String name) {
        for (int i = 0; i < columns.length; i++) {
            if (name.equalsIgnoreCase(columns[i].getName())) {
                return columns[i];
            }
        }
        return null;
    }

    /**
     * Returns the columns of this table that are imported from
     * some other table.
     *
     * @return    Array of imported columns
     */
    public MetaColumn[] getImportedColumns() {
        ArrayList result = new ArrayList(columns.length);

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isImported()) {
                result.add(columns[i]);
            }
        }

        return getColumns(result);
    }

    /**
     * Returns the tables that contain columns this table imports.
     * Essentially a convenience method.
     *
     * @return    Set of tables containing columns we import
     */
    public MetaTable[] getImportedTables() {
        HashSet tables = new HashSet();

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isImported()) {
                tables.add(columns[i].getImportTable());
            }
        }

        return getTables(tables);
    }

    /**
     * Returns the columns of this table that are imported from
     * given table. Essentially a convenience method.
     *
     * @param     table from where the columns must be imported
     * @return    Columns of this table imported from given table
     */
    public MetaColumn[] getColumnsImportedFrom(MetaTable table) {
        ArrayList result = new ArrayList(columns.length);

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getImportTable() == table) {
                result.add(columns[i]);
            }
        }

        return getColumns(result);
    }

    /**
     * Returns the columns of this table that are exported to
     * other tables.
     *
     * @return    Array of exported columns
     */
    public MetaColumn[] getExportedColumns() {
        ArrayList result = new ArrayList(columns.length);

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isExported()) {
                result.add(columns[i]);
            }
        }

        return getColumns(result);
    }

    /**
     * Returns true if this table contains exported columns.
     *
     * @return    True if this table contains exported columns
     */
    public boolean hasExports() {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isExported()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an array of tables that import columns from this table.
     *
     * @return    Array of tables importing columns from this table
     */
    public MetaTable[] getImportingTables() {
        HashSet tables = new HashSet();

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isExported()) {
                MetaColumn[] exports = columns[i].getExportColumns();
                for (int j = 0; j < exports.length; j++) {
                    tables.add(exports[j].getTable());
                }
            }
        }

        return getTables(tables);
    }

    /**
     * Returns an array of columns that are imported from this table.
     *
     * @return    Array of columns imported from this table
     */
    public MetaColumn[] getImportingColumns() {
        HashSet columnSet = new HashSet();

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isExported()) {
                MetaColumn[] exports = columns[i].getExportColumns();
                for (int j = 0; j < exports.length; j++) {
                    columnSet.add(exports[j]);
                }
            }
        }

        return getColumns(columnSet);
    }

    /**
     * Returns the columns that contain LOB-values. (That is, columns
     * for which isLob() returns true.)
     *
     * @return Lob-columns for this table
     */
    public MetaColumn[] getLobColumns() {
        ArrayList result = new ArrayList(columns.length);

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isLob()) {
                result.add(columns[i]);
            }
        }

        return getColumns(result);
    }

    /**
     * Checks if the table contains columns with LOB-values.
     *
     * @return True if table contains lob-columns
     */
    public boolean hasLobColumns() {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isLob()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the sequences used by the columns of this table.
     *
     * @return   Set of sequences used by the columns of this table
     */
    public MetaSequence[] getSequences() {
        HashSet result = new HashSet();

        for (int i = 0; i < columns.length; i++) {
            MetaSequence sequence = columns[i].getSequence();
            if (sequence != null) {
                result.add(sequence);
            }
        }

        return (MetaSequence[])result.toArray(new MetaSequence[result.size()]);
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
     * Compares this table to another table. Ordering is
     * the ordering of the names.
     *
     * @param  obj Object to compare this object against
     * @return a negative integer, zero, or a positive integer as this
     *         table's name is less than, equal to, or greater than
     *         the specified table's name.
     */
    public int compareTo(Object obj) {
        MetaTable table = (MetaTable)obj;
        return name.compareTo(table.name);
    }

    /**
     * Set the columns of this table.
     * <p>
     * This method is package-private and should only be called
     * by <code>MetaDataProvider</code> when constructing tables.
     * <p>
     * This method may only be called once.
     *
     * @param     columns for this table
     * @exception IllegalArgumentException if columns is null
     * @exception IllegalStateException if columns are already set
     */
    void setColumns(MetaColumn[] columns) {
        if (this.columns != null) {
            throw new IllegalStateException("columns already set");
        }
        if (columns == null) {
            throw new IllegalArgumentException("null columns");
        }
        this.columns = columns;
    }

    /**
     * Converts a list of <code>MetaTable</code>s to an array of tables.
     *
     * @param     list of tables to convert
     * @return    Array of MetaTables
     * @exception NullPointerException if list is null
     */
    private static final MetaTable[] getTables(Collection list) {
        return (MetaTable[])list.toArray(new MetaTable[list.size()]);
    }

    /**
     * Converts a list of <code>MetaColumn</code>s to an array of tables.
     *
     * @param     list of columns to convert
     * @return    Array of MetaColumns
     * @exception NullPointerException if list is null
     */
    private static final MetaColumn[] getColumns(Collection list) {
        return (MetaColumn[])list.toArray(new MetaColumn[list.size()]);
    }
}

