/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/repository/java/com/distocraft/dc5000/repository/rockMaker/ContentData.java,v 1.1 2006/04/03 10:33:21 savinen Exp $
 *
 * $Author: savinen $ $Date: 2006/04/03 10:33:21 $
 *
 * $Log: ContentData.java,v $
 * Revision 1.1  2006/04/03 10:33:21  savinen
 * rockmaker and dwhrep metadata added
 *
 * Revision 1.2  2003/05/14 17:15:41  tommi
 * - the old rock v. 1.9 (with Factories) stuff removed
 * - package and imports updated from com.sensorsc to com.solita
 *
 * Revision 1.1  2003/05/14 15:26:42  tommi
 * - initial for rockengine-2.2
 *
 * Revision 1.3  2003/04/24 12:38:07  tommi
 * - some variable visibility changes
 *
 * Revision 1.2  2002/05/22 11:35:02  komu
 * Cleanups.
 *
 * Revision 1.1  2001/08/28 05:41:19  jukka
 * New package structure
 *
 * Revision 1.1  2001/02/02 13:09:59  peik
 * Moved source code files from src to src/com/sensorsc/jagger
 *
 * Revision 1.1  2001/01/15 08:30:35  mikko
 * Initial revision. Class is used for defining sequnce/content columns.
 *
 *
 */

package com.distocraft.dc5000.repository.rockMaker;

import java.util.StringTokenizer;

/**
 * This class posses information for one row in the 'Content column'
 * JaggerTable. Empty cells/members are described with tag EMPTY.
 * <P>
 * Original Author: Mikko Simula
 * @author          $Author: savinen $
 */
public class ContentData {

    public static final int TABLE_INDEX = 0;
    public static final int COLUMN_INDEX = 1;

    /** Used by StringTokenizer */
    protected static final String DELIM = "|";
    /** Contents of empty member */
    protected static final String EMPTY = "###empty###";
    /** The table */
    protected String m_table = EMPTY;
    /** The column of the table that is used as content column */
    protected String m_column = EMPTY;

    /**
     * Constructor
     */
    public ContentData() {}

    /**
     * Constructor
     * 
     * @param table
     * @param column
     */
    public ContentData(String table, String column) {
        if (table != null) {
            if (table.equals("")) {
                table = EMPTY;
            }
            m_table = table;
        }
        if (column != null) {
            if (column.equals("")) {
                column = EMPTY;
            }
            m_column = column;
        }
    }

    /**
     * Constructor
     *
     * @param data The members of object in form of 'xxx[DELIM]table[DELIM]column[DELIM]'
     */
    public ContentData(String data) {
        if (data != null) {
            StringTokenizer st = new StringTokenizer(data, DELIM);
            if (st.hasMoreTokens()) {
                st.nextToken();
            }
            if (st.hasMoreTokens()) {
                m_table = st.nextToken();
            }
            if (st.hasMoreTokens()) {
                m_column = st.nextToken();
            }
        }
    }

    /**
    * Overloaded method toString
    *
    * @return Returns [DELIM]table[DELIM]column[DELIM]
    */
    public String toString() {
        String tmp = DELIM;
        tmp += m_table;
        tmp += DELIM;
        tmp += m_column;
        tmp += DELIM;
        return tmp;
    }

    // All set methods are below

    public void setTable(String table) {
        if (table != null) {
            if (table.equals(""))
                table = EMPTY;
            m_table = table;
        }
    }

    public void setColumn(String column) {
        if (column != null) {
            if (column.equals(""))
                column = EMPTY;
            m_column = column;
        }
    }

    public void setColumn(Object value, int column) {
        if (value != null) {
            if (value.equals(""))
                value = EMPTY;
            switch (column) {
                case TABLE_INDEX :
                    m_table = (String) value;
                    break;
                case COLUMN_INDEX :
                    m_column = (String) value;
                    break;
                default :
                    break;
            }
        }
    }

    // All get methods are below

    public String getTable() {
        return m_table;
    }

    public String getColumn() {
        return m_column;
    }

    public Object getColumn(int column) {
        switch (column) {
            case TABLE_INDEX :
                if (m_table.equals(EMPTY))
                    return (Object) new String();
                else
                    return (Object) m_table;
            case COLUMN_INDEX :
                if (m_column.equals(EMPTY))
                    return (Object) new String();
                else
                    return (Object) m_column;
            default :
                return null;
        }
    }
}