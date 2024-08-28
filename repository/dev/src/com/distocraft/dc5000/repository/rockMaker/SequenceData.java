/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/repository/java/com/distocraft/dc5000/repository/rockMaker/SequenceData.java,v 1.1 2006/04/03 10:33:21 savinen Exp $
 *
 * $Author: savinen $ $Date: 2006/04/03 10:33:21 $
 *
 * $Log: SequenceData.java,v $
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
 * Revision 1.8  2003/04/24 12:39:09  tommi
 * - done some beautification
 *
 * Revision 1.7  2003/03/14 15:03:48  tommi
 * - templates for IDGenerators changed
 *
 * Revision 1.6  2003/03/06 17:11:10  tommi
 * - Generator Strings changed
 *
 * Revision 1.5  2003/03/06 10:58:53  tommi
 * - just cleaned a bit
 *
 * Revision 1.4  2003/02/28 16:24:51  tommi
 * - added "sequence values" for IDGenerators
 *
 * Revision 1.3  2002/06/06 09:47:52  komu
 * Cleanups.
 *
 * Revision 1.2  2002/05/22 11:35:02  komu
 * Cleanups.
 *
 * Revision 1.1  2001/08/28 05:41:19  jukka
 * New package structure
 *
 * Revision 1.1  2001/02/02 13:10:00  peik
 * Moved source code files from src to src/com/sensorsc/jagger
 *
 * Revision 1.1  2001/01/15 08:30:35  mikko
 * Initial revision. Class is used for defining sequnce/content columns.
 *
 */
package com.distocraft.dc5000.repository.rockMaker;

import java.util.StringTokenizer;

/**
 * This class posses information for one row in the 'Sequence column'
 * JaggerTable. Extends ContentData. <P>
 *
 * Original Author: Mikko Simula
 *
 * @author   $Author: savinen $
 * @since    JDK1.2.2
 */
public class SequenceData extends ContentData {

    /** Description of the Field */
    public final static int SEQUENCE_INDEX = 2;

    /** Description of the Field */
    public final static String DEFAULT_SEQUENCE = "MAXVALUE";
    /** Description of the Field */
    public final static String UUID_GENERATOR = "UUIDGenerator(boolean useIp:boolean useHashCode)";
    /** Description of the Field */
    public final static String SEQUENCE_GENERATOR = "SequenceGenerator(String sequenceName)";
    /** Description of the Field */
    public final static String SEQUENCE_BLOCK_GENERATOR = "SequenceBlockGenerator(String sequenceName:String siteIdPart:boolean useRandomPart:int lowKeyUpperValue)";

    /** The table */
    String m_table = EMPTY;

    /** The column of the table that is sequenced */
    String m_column = EMPTY;

    /** The used sequence */
    String m_sequence = DEFAULT_SEQUENCE;

    /** Constructor  */
    public SequenceData() { }

    /**
     * Constructor
     *
     * @param table     Description of the Parameter
     * @param column    Description of the Parameter
     * @param sequence  Description of the Parameter
     */
    public SequenceData(String table, String column, String sequence) {
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
        if (sequence != null) {
            if (sequence.equals("")) {
                sequence = EMPTY;
            }
            m_sequence = sequence;
        }
    }

    /**
     * Constructor
     *
     * @param data  The members of object in form of
     *      'xxx[DELIM]table[DELIM]column[DELIM]sequence[DELIM]'
     */
    public SequenceData(String data) {
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
            if (st.hasMoreTokens()) {
                m_sequence = st.nextToken();
            }
        }
    }

    // All set methods are below

    /**
     * Sets the table attribute of the SequenceData object
     *
     * @param table  The new table value
     */
    public void setTable(String table) {
        if (table != null) {
            if (table.equals("")) {
                table = EMPTY;
            }
            m_table = table;
        }
    }

    /**
     * Sets the column attribute of the SequenceData object
     *
     * @param column  The new column value
     */
    public void setColumn(String column) {
        if (column != null) {
            if (column.equals("")) {
                column = EMPTY;
            }
            m_column = column;
        }
    }

    /**
     * Sets the sequence attribute of the SequenceData object
     *
     * @param sequence  The new sequence value
     */
    public void setSequence(String sequence) {
        if (sequence != null) {
            if (sequence.equals("")) {
                sequence = EMPTY;
            }
            m_sequence = sequence;
        }
    }

    /**
     * Sets the column attribute of the SequenceData object
     *
     * @param value   The new column value
     * @param column  The new column value
     */
    public void setColumn(Object value, int column) {
        if (value != null) {
            if (value.equals("")) {
                value = EMPTY;
            }
            switch (column) {
                case TABLE_INDEX:
                    m_table = (String)value;
                    break;
                case COLUMN_INDEX:
                    m_column = (String)value;
                    break;
                case SEQUENCE_INDEX:
                    m_sequence = (String)value;
                    break;
                default:
                    break;
            }
        }
    }

    // All get methods are below

    /**
     * Gets the table attribute of the SequenceData object
     *
     * @return   The table value
     */
    public String getTable() {
        return m_table;
    }

    /**
     * Gets the column attribute of the SequenceData object
     *
     * @return   The column value
     */
    public String getColumn() {
        return m_column;
    }

    /**
     * Gets the sequence attribute of the SequenceData object
     *
     * @return   The sequence value
     */
    public String getSequence() {
        return m_sequence;
    }

    /**
     * Gets the column attribute of the SequenceData object
     *
     * @param column  Description of the Parameter
     * @return        The column value
     */
    public Object getColumn(int column) {
        switch (column) {
            case TABLE_INDEX:
                if (m_table.equals(EMPTY)) {
                    return new String();
                } else {
                    return m_table;
                }
            case COLUMN_INDEX:
                if (m_column.equals(EMPTY)) {
                    return new String();
                } else {
                    return m_column;
                }
            case SEQUENCE_INDEX:
                if (m_sequence.equals(EMPTY)) {
                    return new String();
                } else {
                    return m_sequence;
                }
            default:
                return null;
        }
    }

    /**
     * Overloaded method toString
     *
     * @return   Returns [DELIM]table[DELIM]column[DELIM]sequence[DELIM]
     */
    public String toString() {
        String tmp = DELIM;
        tmp += m_table;
        tmp += DELIM;
        tmp += m_column;
        tmp += DELIM;
        tmp += m_sequence;
        tmp += DELIM;
        return tmp;
    }
}
