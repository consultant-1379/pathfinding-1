/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/repository/java/com/distocraft/dc5000/repository/rockMaker/MetaDataProvider.java,v 1.1 2006/04/03 10:33:21 savinen Exp $  
 * 
 * 
 * Copyright (c) 2002 Solita Oy. 
 * All rights reserved. 
 *
 * $Author: savinen $ $Date: 2006/04/03 10:33:21 $ 
 * 
 */
package com.distocraft.dc5000.repository.rockMaker;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;



/**
 * Provides high-lever meta-data for given <code>DatabaseMetaData</code>.
 * <p>
 * Original Author:     Juha Komulainen
 * @author              $Author: savinen $
 * @see                 DatabaseMetaData
 */
public class MetaDataProvider {



    /** Map from upper-cased table-names to tables */
    private final HashMap tableMap = new HashMap();

    /** Tables of in the database */
    private final MetaTable[] tables;
    
    /**
     * Constructs new MetaDataProvider for given database-connection,
     * using given catalog, schema pattern and table-name patterns.
     * <p>
     * <code>tableNamePattern</code> may contain several sub-patterns
     * separated by comma. (eg. "EMP, DEPT, FOO_%").
     *
     * @param     metaData Used for accessing the database's metadata
     * @param     catalog to use in searches (may be null)
     * @param     schemaPattern Pattern of included schemas (may be null)
     * @param     tableNamePatterns Array of table-name patterns
     * @param     sequenceColumns List of column-to-sequence -mappings to use
     * @param     useRemarks Whether to fetch remarks on tables and columns?
     * @param     progressListener Listens the fetching progress (may be null)
     * @throws SQLException if something fails with database
     * @throws IllegalArgumentException if metaData is null
     */
    public MetaDataProvider(DatabaseMetaData metaData,
                            String catalog,
                            String schemaPattern,
                            String tableNamePatterns,
                            Collection sequenceColumns,
                            boolean useRemarks)
        throws SQLException
    {
        this(metaData, catalog, schemaPattern, split(tableNamePatterns),
                sequenceColumns, useRemarks);
    }

    /**
     * Constructs new MetaDataProvider for given database-connection,
     * using given catalog, schema pattern and table-name patterns.
     * <p>
     * <code>tableNamePattern</code> may contain several sub-patterns
     * separated by comma. (eg. "EMP, DEPT, FOO_%").
     *
     * @param     metaData Used for accessing the database's metadata
     * @param     catalog to use in searches (may be null)
     * @param     schemaPattern Pattern of included schemas (may be null)
     * @param     tableNamePatterns Comma-separator list of table-name patterns
     * @param     sequenceColumns List of column-to-sequence -mappings to use
     * @param     useRemarks Whether to fetch remarks on tables and columns?
     * @throws SQLException if something fails with database
     * @throws IllegalArgumentException if metaData is null
     */
    public MetaDataProvider(DatabaseMetaData metaData,
                            String catalog,
                            String schemaPattern,
                            String[] tableNamePatterns,
                            Collection sequenceColumns,
                            boolean useRemarks)
        throws SQLException
    {
        if (metaData == null) {
            throw new IllegalArgumentException("null metaData");
        }

        // Construct new builder for meta-data.
        MetaDataBuilder builder = new MetaDataBuilder(
            metaData, catalog, schemaPattern, tableNamePatterns,
            sequenceColumns, useRemarks);

        // First create the tables and their columns.
        tables = builder.getTables();

        // Initialize the table-map
        for (int i = 0; i < tables.length; i++) {
            tableMap.put(tables[i].getName().toUpperCase(), tables[i]);
        }
    }

    /**
     * Returns list of tables matching the catalog, schema
     * and table-name patterns defined for this provider.
     *
     * @return    List of table metadata.
     */
    public MetaTable[] getTables() {
        return (MetaTable[])tables.clone();
    }

    /**
     * Returns the whose name matches given name case-insensitively,
     * or null if no table matches.
     *
     * @return    Table with given name or null
     * @throws NullPointerException if name is null
     */
    public MetaTable getTable(String name) {
        return (MetaTable)tableMap.get(name.toUpperCase());
    }

    /**
     * Returns the given column in given table or null if no
     * table/column with given name exists. Matching of names
     * ignores case.
     *
     * @param     tableName Name of the table where the column is searched
     * @param     columnName Name of the column to return
     * @return    Column matching given criterias or null
     * @throws NullPointerException if one of names is null
     */
    public MetaColumn getColumn(String tableName, String columnName) {
        MetaTable table = getTable(tableName);
        if (table != null) {
            return table.getColumn(columnName);
        } else {
            return null;
        }
    }

    /**
     * Returns array containing meta-data for all columns.
     *
     * @return List of all columns
     */
    public MetaColumn[] getAllColumns() {
        ArrayList result = new ArrayList();
        for (int i = 0; i < tables.length; i++) {
            MetaColumn[] columns = tables[i].getColumns();
            for (int j = 0; j < columns.length; j++) {
                result.add(columns[j]);
            }
        }
        return (MetaColumn[])result.toArray(new MetaColumn[result.size()]);
    }

    /**
     * Splits given string between commas and return the results
     * in an array. Trim the array-items.
     */
    private static String[] split(String str) {
        if (str == null) {
            return null;
        }

        if (str.indexOf(',') != -1) {
            StringTokenizer st = new StringTokenizer(str, ",");
            String[] array = new String[st.countTokens()];
            for (int i = 0; i < array.length; i++) {
                array[i] = st.nextToken().trim();
            }
            return array;
        } else {
            return new String[] { str };
        }
    }

}

/**
 * Class for actually constructing the metadata.
 * <p>
 * Original Author:     Juha Komulainen
 * @author              $Author: savinen $
 * @see                 MetaDataProvider
 */
class MetaDataBuilder {

    /** The types of tables to construct */
    private static final String[] TABLE_TYPE_LIST = {
        "TABLE", "BASE TABLE", "VIEW"
    };

    /** Meta-data object used for accessing database */
    private final DatabaseMetaData metaData;

    /** Catalog to use for accessing tables (may be null) */
    private final String catalog;

    /** Schema to use for accessing tables (may be null) */
    private final String schemaPattern;

    /** List of table-name patterns */
    private final String[] tableNamePatterns;

    /** Do we want to get the remarks for tables and columns? */
    private final boolean useRemarks;

    /** Map from fully qualified column names to corresponding sequences */
    private final HashMap sequenceMap = new HashMap();


    

    /**
     * Construct new meta-data builder with given arguments.
     *
     * @param     metaData used for accessing low-level metadata
     * @param     catalog to use for accessing tables (may be null)
     * @param     schema to use for accessing tables (may be null)
     * @param     tableNamePatterns Patterns of table names to include
     *            (may be null to include all)
     * @param     sequenceColumns List of column-to-sequence -mappings to use
     * @param     useRemarks Whether to fetch remarks on tables and columns?
     * @param     progressListener Listens the fetching progress (may be null)
     * @throws IllegalArgumentException if metaData is null
     */
    MetaDataBuilder(DatabaseMetaData metaData,
                    String catalog,
                    String schemaPattern,
                    String[] tableNamePatterns,
                    Collection sequenceColumns,
                    boolean useRemarks) {
        if (metaData == null) {
            throw new IllegalArgumentException("null metaData");
        }

        this.metaData = metaData;
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.useRemarks = useRemarks;

        if (tableNamePatterns != null) {
            this.tableNamePatterns = tableNamePatterns;
        } else {
            this.tableNamePatterns = new String[] { null };
        }


        // Initialize the sequenceMap from given collection
        if (sequenceColumns != null) {
            Iterator iter = sequenceColumns.iterator();
            while (iter.hasNext()) {
                SequenceData data = (SequenceData)iter.next();

                String name = null;
                
                String sequence = data.getSequence();
                if (sequence.indexOf("(") != -1) {
                    name = getIDGeneratorString(sequence);
                }
                else {
                    name = sequence.toUpperCase();
                }
                String table = data.getTable().toUpperCase();
                String column = data.getColumn().toUpperCase();
                String key = table + '.' + column;
                sequenceMap.put(key, new MetaSequence(name));
            }
        }
    }

    /**
     * Create the tables for the database. This fetches the tables
     * and columns and then sets the imports correctly for columns.
     *
     * @return Tables of the database
     * @throws SQLException
     */
    public MetaTable[] getTables() throws SQLException {
        // Set the state of remarks-reporting...
        setConnectionRemarksReporting();

        // First create the tables
        MetaTable[] tables = createTables();

        // Now create a mapping on the tables
        HashMap tableMap = new HashMap();
        for (int i = 0; i < tables.length; i++) {
            tableMap.put(tables[i].getName().toUpperCase(), tables[i]);
        }

        // Now initialize the imports for each table
        for (int i = 0; i < tables.length; i++) {
            initializeImports(tableMap, tables[i]);
        }

        return tables;
    }

    /**
     * Create the tables for the databse and add columns to the
     * tables. Everything else is done after call to this method,
     * except that the columns of returned tables do not have
     * their imports/exports set.
     *
     * @return Tables of the database
     * @throws SQLException
     */
    private MetaTable[] createTables() throws SQLException {
        ArrayList result = new ArrayList();

        // Loop through each pattern
        for (int i = 0; i < tableNamePatterns.length; i++) {
            String pattern = tableNamePatterns[i];
            ResultSet resultSet = metaData.getTables(
                catalog, schemaPattern, pattern, TABLE_TYPE_LIST);

            while (resultSet.next()) {
                result.add(createTable(resultSet));
            }
        }

        return (MetaTable[])result.toArray(new MetaTable[result.size()]);
    }

    /**
     * Initialize the imports on given table.
     *
     * @param  tableMap Map of uppercased table-names to tables
     * @param  table whose imports to initialize
     * @throws SQLException
     */
    private void initializeImports(Map tableMap, MetaTable table)
                throws SQLException {
        String tableName = table.getName();
        ResultSet impResults = metaData.getImportedKeys(
                table.getCatalog(), table.getSchema(), tableName);

        while (impResults.next()) {
            String columnName = impResults.getString("FKCOLUMN_NAME");
            String impTable   = impResults.getString("PKTABLE_NAME");
            String impColumn  = impResults.getString("PKCOLUMN_NAME");

            MetaColumn column   = table.getColumn(columnName);
            MetaColumn imported = getColumn(tableMap, impTable, impColumn);

            // If the imported table is not included in the selected tables,
            // we ignore the relationship, since we can't use it anyhow.
            if (imported != null) {
                column.setImportFromColumn(imported);
            }
        }
    }

    /**
     * Returns the given column from given table where map
     * is a mapping from uppercased table-names to tables.
     * 
     * @param map
     * @param tableName
     * @param columnName
     * @return column for given table or null
     */
    private static MetaColumn getColumn(Map map,
                                        String tableName,
                                        String columnName) {
        MetaTable table = (MetaTable)map.get(tableName.toUpperCase());
        if (table != null) {
            return table.getColumn(columnName);
        } else {
            return null;
        }
    }

    /**
     * Creates a table from row in given ResultSet. Also creates
     * the columns for this table.
     *
     * @param     resultSet Result-set whose row is used for values of table
     * @return    Newly created table
     * @throws SQLException
     */
    private MetaTable createTable(ResultSet resultSet) throws SQLException {
        // First get the name of this table and fire a progress-event
        String name = resultSet.getString("TABLE_NAME");

        // Now get the rest of the table data and instantiate it
        String catalog = resultSet.getString("TABLE_CAT");
        String schema  = resultSet.getString("TABLE_SCHEM");
        String type    = resultSet.getString("TABLE_TYPE");
        String remarks = getRemarks(resultSet);

        // Create the table with given name and set the columns for it
        MetaTable table = new MetaTable(name, catalog, schema, type, remarks);
        table.setColumns(createColumns(table));

        // Finally return the table
        return table;
    }

    /**
     * Create columns for given table, but does not attach them to the
     * table.
     *
     * @param     table whose columns to create
     * @return    Columns for given table
     * @throws SQLException
     */
    private MetaColumn[] createColumns(MetaTable table) throws SQLException {
        // Get the set of primary keys
        HashSet primaryKeySet = new HashSet();
        ResultSet primaryKeyResults = metaData.getPrimaryKeys(
                table.getCatalog(), table.getSchema(), table.getName());
        while (primaryKeyResults.next()) {
            primaryKeySet.add(primaryKeyResults.getString("COLUMN_NAME"));
        }

        // Now get the set of all columns and process them
        ArrayList result = new ArrayList();
        ResultSet resultSet = metaData.getColumns(
                table.getCatalog(), table.getSchema(), table.getName(), "%");

        // If we use Oracle db this  guarentees that the default values are generated,
        // because jdbc driver for Oracle is not currently supporting the default 
        // values
        Map columnsAndDefaults = new HashMap(0);
       
        /*
        try {
            columnsAndDefaults = dataSeeker.getDefaultsFromDB(table.getName());
        } catch (Exception ignore) {
            ignore.printStackTrace();
            columnsAndDefaults = new HashMap(0);
        }
        */
        // Process all columns
        while (resultSet.next()) {
            result.add(createColumn(table, resultSet, primaryKeySet, 
                        columnsAndDefaults));
        }

        return (MetaColumn[])result.toArray(new MetaColumn[result.size()]);
    }

    /**
     * Create column for given table from row in given ResultSet.
     *
     * @param     table The table for this column
     * @param     resultSet used for columns attributes
     * @param     primaryKeySet names of primary-keys for this table
     * @param     columnsAndDefaultValues Default values for columns, 
     *             key is column name
     * @return    New column
     * @throws SQLException
     */
    private MetaColumn createColumn(MetaTable table,
                                    ResultSet resultSet,
                                    Set primaryKeySet,
                                    Map columnsAndDefaultValues)
                throws SQLException {

        String name         = resultSet.getString("COLUMN_NAME");
        boolean isPrimary  = primaryKeySet.contains(name);
        boolean isNullable = !"NO".equals(resultSet.getString("IS_NULLABLE"));
        short dataType     = resultSet.getShort("DATA_TYPE");
        String typeName     = resultSet.getString("TYPE_NAME");
        int decimalDigits   = resultSet.getInt("DECIMAL_DIGITS");
        int columnSize      = resultSet.getInt("COLUMN_SIZE");
        String defaultValue = getString(resultSet, "COLUMN_DEF", null);
        int ordinalPosition   = resultSet.getInt("ORDINAL_POSITION") - 1;
        if (defaultValue == null) {
            defaultValue = (String)columnsAndDefaultValues.get(name);
        }

        String remarks      = getRemarks(resultSet);
        MetaSequence seq    = getSequence(table.getName(), name);

        return new MetaColumn(table, name, dataType, typeName,
                        defaultValue, isNullable, isPrimary, decimalDigits,
                        columnSize, seq, remarks, ordinalPosition);
    }


    /**
     * Returns sequence for specified column or null
     *
     * @param     tableName Table to which the column belong
     * @param     columnName Name of the column to use
     * @return    Sequence for the column or null
     */
    private MetaSequence getSequence(String tableName, String columnName) {
        String key = tableName.toUpperCase() + '.' + columnName.toUpperCase();
        return (MetaSequence)sequenceMap.get(key);
    }

    /**
     * Set the remarks reporting on for the connection.
     * <p>
     * Note that currently only Oracle is fully supported, using
     * remarks with other databases may or may not work, depending
     * whether fetching remarks is on by default.
     *
     * @throws SQLException
     */
    private void setConnectionRemarksReporting() throws SQLException {

    	/*
    	Connection connection = metaData.getConnection();

        // Do we have Oracle? Compare the name instead checking with
        // instanceof so we don't have to load the class unless we
        // really need it. (We might not have Oracle-classes in path.)
        if (connection.getClass().getName().indexOf("OracleConnection") > 0) {
            if (connection instanceof OracleConnection) {
                OracleConnection orc = (OracleConnection)connection;
                orc.setRemarksReporting(useRemarks);
            }
        }
        
        */
    }

    /**
     * Returns the remarks from given result-set if 'useRemarks'-flag
     * is true. Otherwise returns null.
     */
    private String getRemarks(ResultSet set) {
        if (useRemarks) {
            return getString(set, "REMARKS", null);
        } else {
            return null;
        }
    }
    
    /**
     * Creates the ID generator string for the template
     * 
     * @param sequenceData Contains the generator name,params and return type
     * @return String
     */
    private String getIDGeneratorString(String sequenceData) {
        String[] threeParts = split(sequenceData, "()");
        String[] parameters = split(threeParts[1], ":");
        
        StringBuffer buffer = new StringBuffer(sequenceData.length());
        // The generator name
        buffer.append(threeParts[0].trim());
        buffer.append("(");
        
        for (int i = 0; i < parameters.length; i++) {
            buffer.append(parameters[i].trim());
            if (i < parameters.length - 1) {
                buffer.append(":");
            }
        }

        buffer.append(")");
        
        return buffer.toString();
    }

    /**
     * Returns the value defined by given key on result-set or
     * given default is it does not exist. Convenience method.
     */
    private static String getString(ResultSet set, String key, String def) {
        try {
            return set.getString(key);
        } catch (SQLException e) {
            return def;
        }
    }
    
    /**
     * Splits given string between commas and return the results
     * in an array. Trim the array-items.
     */
    private static String[] split(String str, String delims) {
        if (str == null) {
            return null;
        }
        if (delims == null) {
            return new String[] { str };
        }

        if (str.indexOf(delims.charAt(0)) != -1) {
            StringTokenizer st = new StringTokenizer(str, delims);
            String[] array = new String[st.countTokens()];
            for (int i = 0; i < array.length; i++) {
                array[i] = st.nextToken().trim();
            }
            return array;
        } else {
            return new String[] { str };
        }
    }
}
/*
 * Revision History:
 * 
 * $Log: MetaDataProvider.java,v $
 * Revision 1.1  2006/04/03 10:33:21  savinen
 * rockmaker and dwhrep metadata added
 *
 * Revision 1.4  2003/09/10 12:02:39  tommi
 * - nothing
 *
 * 
 * Revision 1.3  2003/05/16 21:35:05  tommi
 * - now fetches also the ordinalNumber
 *
 * Revision 1.2  2003/05/14 17:14:35  tommi
 * - package and imports updated from com.sensorsc to com.solita
 *
 * Revision 1.1  2003/05/14 15:26:42  tommi
 * - initial for rockengine-2.2
 *
 * Revision 1.5  2003/03/14 15:04:36  tommi
 * - removed return from IDGenerator string
 *
 * Revision 1.4  2003/03/06 17:15:44  tommi
 * - added support for IDGenerators: put into the columnsAndSequences
 * - added method getIDGeneratorString(String):String
 *
 * Revision 1.3  2003/03/06 11:02:53  tommi
 * - added parameter AdditionalDataSeeker to constructor
 * - If the defaults are not returned with metadata, they are fetched from db.
 *   Works only with Oracle
 *
 * Revision 1.2  2002/03/21 09:04:11  komu
 * Fixed exception when there was an import to table that was not used.
 *
 * Revision 1.1  2002/03/01 12:07:00  komu
 * Moved.
 *
 */
