/*
 * @(#) $Header: /usr/cvsrepository/dc5000/src/repository/java/com/distocraft/dc5000/repository/rockMaker/MetaSequence.java,v 1.1 2006/04/03 10:33:21 savinen Exp $
 *
 *
 * $Author: savinen $ $Date: 2006/04/03 10:33:21 $
 *
 * $Log: MetaSequence.java,v $
 * Revision 1.1  2006/04/03 10:33:21  savinen
 * rockmaker and dwhrep metadata added
 *
 * Revision 1.2  2003/05/14 17:14:35  tommi
 * - package and imports updated from com.sensorsc to com.solita
 *
 * Revision 1.1  2003/05/14 15:26:42  tommi
 * - initial for rockengine-2.2
 *
 * Revision 1.2  2002/03/22 12:28:11  komu
 * Implement Comparable.
 *
 * Revision 1.1  2002/03/01 12:06:48  komu
 * Initial revision.
 *
 *
 */

package com.distocraft.dc5000.repository.rockMaker;

/**
 *
 * Contains meta-data for a sequence of a database.
 * <p>
 * MetaSequences are immutable after they have been constructed
 * by <code>MetaDataProvider</code> and only one instance
 * is created for each sequence in the database.
 * <p>
 * This may be a poor-man's view to sequences, but it's all
 * we currently need.
 * <p>
 * Original Author:     Juha Komulainen
 * @author              $Author: savinen $
 * @since               JDK 1.2
 */
public final class MetaSequence implements Comparable {

    /** Name of the sequence */
    private final String name;

    /**
     * Constructs new <code>MetaSequence</code> with given arguments.
     *
     * @param  name Name of the sequence
     * @throws IllegalArgumentException if some of the arguments is illegal
     */
    MetaSequence(String name) {
        if (name == null)    throw new IllegalArgumentException("null name");
        if (name.equals("")) throw new IllegalArgumentException("empty name");

        this.name = name;
    }

    /**
     * Returns tbe name of the sequence.
     *
     * @return Name of the sequence
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the name of this sequence where first character is
     * upper-case and rest are lower-cased.
     *
     * @return    Name of this sequence capitalized
     */
    public String getCapitalizedName() {
        // We ensured in constructor that 'name' is at least 1 char in length.
        return Character.toUpperCase(name.charAt(0))
               + name.substring(1).toLowerCase();
    }

    /**
     * Returns a String-representation of this object.
     *
     * @return String-representation of this object
     */
    public String toString() {
        return getCapitalizedName();
    }

    /**
     * Compares this sequence to another sequence. Ordering is
     * the ordering of the names.
     *
     * @param  obj Object to compare this object against
     * @return a negative integer, zero, or a positive integer as this
     *         sequence's name is less than, equal to, or greater than
     *         the specified sequence's name.
     */
    public int compareTo(Object obj) {
        MetaSequence sequence = (MetaSequence)obj;
        return name.compareTo(sequence.name);
    }
}

