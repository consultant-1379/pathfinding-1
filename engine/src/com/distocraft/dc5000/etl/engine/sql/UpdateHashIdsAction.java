/*------------------------------------------------------------------------
 *
 *
 *      COPYRIGHT (C)                   ERICSSON RADIO SYSTEMS AB, Sweden
 *
 *      The  copyright  to  the document(s) herein  is  the property of
 *      Ericsson Radio Systems AB, Sweden.
 *
 *      The document(s) may be used  and/or copied only with the written
 *      permission from Ericsson Radio Systems AB  or in accordance with
 *      the terms  and conditions  stipulated in the  agreement/contract
 *      under which the document(s) have been supplied.
 *
 *------------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.engine.sql;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.*;
import com.distocraft.dc5000.repository.dwhrep.Dwhpartition;
import com.distocraft.dc5000.repository.dwhrep.DwhpartitionFactory;
import com.ericsson.eniq.common.HashIdCreator;

/**
 * This class is used to populate hashing ID columns after the columns have been added to a table in a upgrade. This is done as follows: 1. Check
 * table for NULL hashing ID values. 2. If they exist, write the associated rows for this hashing ID out to a file. 3. While writing the rows out to a
 * file, append correct values for the hashing IDs. 4. Delete the rows with NULL hashing ID values from table. 5. Load the created file into the table
 * with correct hashing ID values.
 * 
 * @author epaujor
 * 
 */
public class UpdateHashIdsAction extends SQLOperation {

    private static final String SET_TEMPORARY_OPTION_ESCAPE_CHARACTER_ON = StaticProperties.getProperty("updateHashIdsAction.temp.option",
            "set temporary option ESCAPE_CHARACTER='ON'; \n");

    private static final String NULL_OPTIONS = " NULL('null', 'NULL')";

    private static final String TMP = StaticProperties.getProperty("updateHashIdsAction.loader.dir", File.separatorChar + "eniq" + File.separatorChar
            + "home");

    private static final String LOADER_OPTIONS = "QUOTES OFF ESCAPES OFF DELIMITED BY '\\x7c' ROW DELIMITED BY  '\\x0a' IGNORE CONSTRAINT UNIQUE 1 STRIP RTRIM;\n";

    private static final String LOAD_TABLE = "LOAD TABLE ";

    private static final String SELECT = "SELECT ";

    private static final String DELETE = "DELETE ";

    private static final String EMPTY_STRING = "";

    private static final String PIPE = "|";

    private static final String NEW_LINE = "\n";

    private static final String COMMA_SEPERATOR = ",";

    private transient final Logger log; // NOPMD

    private transient String targetStorageId;

    private transient final HashIdCreator hIdCreator;

    /**
     * Constructs the UpdateHashIdsAction that will be used to populate hashing ID columns after the columns have been added to a table in a upgrade
     * 
     * @param version
     * @param collectionSetId
     * @param collection
     * @param transferActionId
     * @param transferBatchId
     * @param connectId
     * @param etlreprock
     * @param trActions
     * @param parentlog
     * @param cpool
     * @throws EngineMetaDataException
     * @throws NoSuchAlgorithmException
     */
    public UpdateHashIdsAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
                               final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory etlreprock,
                               final Meta_transfer_actions trActions, final Logger parentlog, final ConnectionPool cpool)
            throws EngineMetaDataException, NoSuchAlgorithmException {

        super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, etlreprock, cpool, trActions);
        log = Logger.getLogger(parentlog.getName() + ".UpdateHashIdsAction");

        if (getTrActions().getWhere_clause() == null || getTrActions().getWhere_clause().length() <= 0) {
            throw new EngineMetaDataException("Target type not set", new Exception(), "init");
        }
        setTargetStorageId();
        hIdCreator = new HashIdCreator();

    }

    /**
     * Executes the Action
     * 
     * @throws SQLException
     * @throws RockException
     */
    @Override
    public void execute() throws EngineException, SQLException, IOException, RockException {
        final String columns = getStorageIdColumns();
        if (columns == null) {
            log.info("No columns available for " + targetStorageId);
        } else {
            final List<String> hashIdCols = getHashIdColumns(columns);

            if (hashIdCols.isEmpty()) {
                log.info("No Hash Id columns available for " + targetStorageId);
            } else {
                updateHashIds(columns, hashIdCols);
            }
        }
    }

    private void updateHashIds(final String columns, final List<String> hashIdCols) throws EngineException, IOException, RockException, SQLException {
        log.finest("Getting list of partitions for storage ID: " + targetStorageId);
        final List<Dwhpartition> partitions = getListOfPartitions();
        log.finest("Got " + partitions.size() + " partition for storage ID: " + targetStorageId);

        for (Dwhpartition partition : partitions) {
            final String targetTable = partition.getTablename();
            log.finest("targetTable = " + targetTable);
            final File hashIdFile = createFileWithNewHashIds(columns, hashIdCols, targetTable);
            if (hashIdFile.exists()) {
                deleteRowWithNoHashIds(hashIdCols, targetTable);
                loadNewHashIdRows(columns, targetTable, hashIdFile);
                hashIdFile.delete();
            }
        }
    }

    protected List<Dwhpartition> getListOfPartitions() throws RockException, SQLException {
        List<Dwhpartition> partitions = new ArrayList<Dwhpartition>();
        RockFactory dwhrepRockFactory = null;
        try {
            dwhrepRockFactory = getPrivateRockFactory(getRockFact(), DWHREP_CONNECTION_NAME, USER);
            final Dwhpartition dwhPartition = new Dwhpartition(dwhrepRockFactory);
            dwhPartition.setStorageid(targetStorageId);
            final DwhpartitionFactory dp_fact = new DwhpartitionFactory(dwhrepRockFactory, dwhPartition);
            partitions = dp_fact.get();
        } catch (SQLException se) {
            log.warning("Could not get any partitions for storage ID: " + targetStorageId);
        } finally {
            if (dwhrepRockFactory != null) {
                dwhrepRockFactory.getConnection().close();
            }
        }
        return partitions;
    }

    /**
     * Creates a file with the new HashId values input into this file
     * 
     * @param columns
     * @param hashIdCols
     * @param targetTable
     * @return
     * @throws EngineException
     * @throws IOException
     */
    protected File createFileWithNewHashIds(final String columns, final List<String> hashIdCols, final String targetTable) throws EngineException,
            IOException {
        Statement stmt = null; // NOPMD - see close() method in finally block
        ResultSet resultSet = null; // NOPMD - see close() method in finally block
        final File hashIdFile = new File(TMP + File.separatorChar + targetTable + ".sql");
        final String sql = generateSql(SELECT, columns, hashIdCols, targetTable);
        try {
            final RockFactory dwhRockFactory = getConnection();
            stmt = dwhRockFactory.getConnection().createStatement();
            resultSet = stmt.executeQuery(sql);

            if (resultSet.next()) {
                createHashIdFile(resultSet, hashIdFile);
                log.fine("Created temporary file: " + hashIdFile + " with hashing IDs for table: " + targetTable);
            } else {
                log.fine("No hashing IDs were updated in table: " + targetTable);
            }
        } catch (SQLException e) {
            log.log(Level.WARNING, "Statement failed: " + sql, e);
            throw new EngineException("Creation of hash ID file failed", e, this, "execute", "ERROR");
        } finally {
            close(resultSet, stmt, log);
        }
        return hashIdFile;
    }

    private void createHashIdFile(final ResultSet resultSet, final File hashIdFile) throws IOException, FileNotFoundException, SQLException {
        hashIdFile.createNewFile();
        final FileOutputStream fileOutputStream = new FileOutputStream(hashIdFile);

        final int numOfCols = resultSet.getMetaData().getColumnCount();

        // Add first to file, resultSet already moved to position of first row
        String rowForFile = createRowForFile(resultSet, numOfCols);
        fileOutputStream.write(rowForFile.getBytes());
        fileOutputStream.flush();

        while (resultSet.next()) {
            // Any rows after first row need a new line added to end of previous line
            fileOutputStream.write(NEW_LINE.getBytes());
            rowForFile = createRowForFile(resultSet, numOfCols);
            fileOutputStream.write(rowForFile.getBytes());
            fileOutputStream.flush();
        }
        fileOutputStream.write(NEW_LINE.getBytes());
        fileOutputStream.close();
    }

    /**
     * Deletes rows that have a hash ID value equal to NULL
     * 
     * @param hashIdCols
     * @param targetTable
     * @throws EngineException
     */
    private void deleteRowWithNoHashIds(final List<String> hashIdCols, final String targetTable) throws EngineException {
        final String sql = generateSql(DELETE, EMPTY_STRING, hashIdCols, targetTable);

        executeDwhUpdate(sql, DELETE);
    }

    /**
     * Loads the file into the table with the correct hashing ID values
     * 
     * @param columns
     * @param targetTable
     * @param hashIdFile
     * @throws EngineException
     */
    private void loadNewHashIdRows(final String columns, final String targetTable, final File hashIdFile) throws EngineException {
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(LOAD_TABLE);
        sqlBuilder.append(targetTable);
        sqlBuilder.append(" (");
        sqlBuilder.append(columns.replaceAll(COMMA_SEPERATOR, NULL_OPTIONS + COMMA_SEPERATOR));
        sqlBuilder.append(NULL_OPTIONS);
        sqlBuilder.append(" ) from '");
        sqlBuilder.append(hashIdFile.getAbsolutePath());
        sqlBuilder.append("' ");
        sqlBuilder.append(LOADER_OPTIONS);
        sqlBuilder.append(";");

        executeDwhUpdate(sqlBuilder.toString(), LOAD_TABLE);
    }

    private void executeDwhUpdate(final String sql, final String logInfo) throws EngineException {
        Statement stmt = null; // NOPMD - see close() method in finally block
        try {
            final RockFactory dwhRockFactory = getConnection();
            stmt = dwhRockFactory.getConnection().createStatement();
            stmt.executeUpdate(SET_TEMPORARY_OPTION_ESCAPE_CHARACTER_ON);
            final int count = stmt.executeUpdate(sql);
            log.fine("Executed query: " + sql);
            log.fine("Rows affected: " + count);
        } catch (SQLException e) {
            log.log(Level.WARNING, "Statement failed: " + sql, e);
            throw new EngineException(logInfo + " failed", e, this, "execute", "ERROR");
        } finally {
            close(null, stmt, log);
        }
    }

    private String createRowForFile(final ResultSet resultSet, final int numOfCols) throws SQLException, IOException {
        final StringBuilder strBuilder = new StringBuilder();

        for (int i = 1; i <= numOfCols; i++) {
            final String colName = resultSet.getMetaData().getColumnName(i);
            final List<String> colsForHashId = hIdCreator.getColsForHashId(colName.toUpperCase(Locale.getDefault()));
            String colValue;

            if (colsForHashId == null) {
                colValue = resultSet.getString(i);
            } else {
                colValue = generateHashIdValue(resultSet, colsForHashId);
            }
            strBuilder.append(colValue);
            strBuilder.append(PIPE);

        }
        return strBuilder.toString();
    }

    private String generateHashIdValue(final ResultSet resultSet, final List<String> colsForHashId) throws SQLException, IOException {
        final Iterator<String> colsIterator = colsForHashId.iterator();
        final StringBuilder strBuilder = new StringBuilder();
        while (colsIterator.hasNext()) {
            final String column = colsIterator.next();
            final String dataForColumn = resultSet.getString(column);
            if (dataForColumn == null) {
                strBuilder.append(EMPTY_STRING);
            } else {
                strBuilder.append(dataForColumn);
            }

            if (colsIterator.hasNext()) {
                strBuilder.append(PIPE);
            }
        }
        final long hashId = hIdCreator.hashStringToLongId(strBuilder.toString());
        return Long.toString(hashId);
    }

    private String generateSql(final String startOfSql, final String columns, final List<String> hashIdCols, final String targetTable) {
        final StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(startOfSql);
        strBuilder.append(columns);
        strBuilder.append(" FROM ");
        strBuilder.append(targetTable);
        strBuilder.append(" WHERE ");
        strBuilder.append(getWhereClause(hashIdCols));
        return strBuilder.toString();
    }

    private String getWhereClause(final List<String> hashIdCols) {
        final StringBuilder strBuilder = new StringBuilder();
        final Iterator<String> hashIdColsIter = hashIdCols.iterator();
        while (hashIdColsIter.hasNext()) {
            strBuilder.append(hashIdColsIter.next());
            strBuilder.append(" is null ");
            if (hashIdColsIter.hasNext()) {
                strBuilder.append("or ");
            }
        }
        return strBuilder.toString();
    }

    private void setTargetStorageId() throws EngineMetaDataException {
        final Properties whereClause = stringToProperties(getTrActions().getWhere_clause());

        // StorageID of target type. Example DIM_E_SGEH_HIER321:PLAIN
        targetStorageId = whereClause.getProperty("targetType");
        log.finest("targetStorageIDs = " + targetStorageId);

        if (targetStorageId == null || targetStorageId.length() <= 0) {
            throw new EngineMetaDataException("Target Storage Id not set", new Exception(), "init");
        }
    }

    /**
     * This method checks to see if there are any hashing ID columns
     * 
     * @param columns
     * @return
     */
    private List<String> getHashIdColumns(final String columns) {
        final List<String> hashIdCols = new ArrayList<String>();
        for (String hashId : hIdCreator.getHashIdColumns()) {
            if (columns.contains(hashId)) {
                hashIdCols.add(hashId);
            }
        }
        return hashIdCols;
    }

    /**
     * This method finds the list of columns for this storageId
     * 
     * @return partitionColumns
     * @throws SQLException
     */
    private String getStorageIdColumns() throws SQLException {
        Statement statement = null; // NOPMD - see close() method in finally block
        ResultSet resultSet = null; // NOPMD - see close() method in finally block
        String columns = null;
        // select dataname from dwhColumn where STORAGEID = 'storageId'
        final StringBuilder sqlColumns = new StringBuilder();
        sqlColumns.append("select dataname");
        sqlColumns.append(" from dwhColumn");
        sqlColumns.append(" where STORAGEID = '");
        sqlColumns.append(targetStorageId);
        sqlColumns.append("'");

        final StringBuilder tableColumns = new StringBuilder();
        log.finest("Getting list of columns.");

        RockFactory dwhrepRockFactory = null;
        try {
            dwhrepRockFactory = getPrivateRockFactory(getRockFact(), DWHREP_CONNECTION_NAME, USER);
            statement = dwhrepRockFactory.getConnection().createStatement();
            resultSet = statement.executeQuery(sqlColumns.toString());

            while (resultSet.next()) {
                tableColumns.append(resultSet.getString("dataname"));
                tableColumns.append(", ");
            }

            final String tableCols = tableColumns.toString();
            if (!tableCols.isEmpty()) {
                columns = tableCols.substring(0, tableCols.lastIndexOf(COMMA_SEPERATOR));
            }
            log.finest("Columns for storageId: " + columns);
        } catch (SQLException se) {
            log.warning(sqlColumns.toString() + " could not be run. Could not get columns for the storageId: " + targetStorageId);
        } finally {
            close(resultSet, statement, log);
            if (dwhrepRockFactory != null) {
                dwhrepRockFactory.getConnection().close();
            }
        }
        return columns;
    }
}
