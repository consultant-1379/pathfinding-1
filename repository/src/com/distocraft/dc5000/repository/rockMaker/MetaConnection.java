/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/repository/java/com/distocraft/dc5000/repository/rockMaker/MetaConnection.java,v 1.1 2006/04/03 10:33:21 savinen Exp $
 *
 *
 * $Author: savinen $ $Date: 2006/04/03 10:33:21 $
 *
 * $Log: MetaConnection.java,v $
 * Revision 1.1  2006/04/03 10:33:21  savinen
 * rockmaker and dwhrep metadata added
 *
 * Revision 1.2  2003/05/14 17:14:35  tommi
 * - package and imports updated from com.sensorsc to com.solita
 *
 * Revision 1.1  2003/05/14 15:26:42  tommi
 * - initial for rockengine-2.2
 *
 * Revision 1.3  2003/03/06 11:00:51  tommi
 * - added parameter AdditionalDataSeeker to constructor
 *
 * Revision 1.2  2002/06/06 09:47:52  komu
 * Cleanups.
 *
 * Revision 1.1  2002/03/04 13:49:07  komu
 * Initial revision.
 *
 *
 */

package com.distocraft.dc5000.repository.rockMaker;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;



/**
 *
 * Convenience method for getting meta-data for database-connection.
 * <p>
 * Original Author:     Juha Komulainen
 * @author              $Author: savinen $
 */
public class MetaConnection {

    /** The database-connection to use */
    private Connection connection = null;

    /**
     * Constructs new MetaConnection to given url using given
     * driver-class and logging in with given username and password.
     *
     * @param  url of the database
     * @param  driverClass Name of the class for the connection
     * @param  userName User name used for logging in
     * @param  password Password use for logging in
     * @throws SQLException
     * @throws InstantiationException
     */
    public MetaConnection(String url, String driverClass,
                          String userName, String password)
            throws SQLException, InstantiationException {

        try {
            Driver driver = (Driver)Class.forName(driverClass).newInstance();
            Properties props = new Properties();
            props.put("user", userName);
            props.put("password", password);

            this.connection = driver.connect(url, props);

        } catch (ClassNotFoundException e) {
            throw new InstantiationException(e.toString());
        } catch (IllegalAccessException e) {
            throw new InstantiationException(e.toString());
        }
    }

    /**
     * Construct new MetaConnection using given connection.
     *
     * @param  connection to use
     * @throws IllegalArgumentException if connection is null
     */
    public MetaConnection(Connection connection) {
        if (connection == null) throw new IllegalArgumentException();

        this.connection = connection;
    }

    /**
     * Returns new MetaDataProvider for this connection.
     *
     * @param  catalog Catalog to fetch meta-data for
     * @param  schema Schema of the metadata
     * @param  tablePattern Names of tables to fetch
     * @param  sequenceColumns Sequence-information
     * @param  useColumnRemarks Fetch column remarks as well?
     * @return New meta-data provider
     */
    public MetaDataProvider getMetaDataProvider(
            String catalog, String schema, String tablePattern,
            Collection sequenceColumns, boolean useColumnRemarks)
                throws SQLException {

        return new MetaDataProvider(
                connection.getMetaData(), catalog, schema,
                tablePattern, sequenceColumns, useColumnRemarks);
    }

    /**
     * Returns metadata for the connection.
     *
     * @return Metadata for the connection
     * @throws SQLException
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

}

