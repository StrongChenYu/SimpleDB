package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.ArrayList;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    private ArrayList<TDItem> tdItems;
    //addAll method
    private int fieldNums;

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;

        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // project 3 will use
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int fieldL = fieldAr.length;
        int typeL = typeAr.length;
        tdItems = new ArrayList<TDItem>();

        //whether fieldL==typeL
        for(int i = 0; i < fieldL; i++){
           tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }

        fieldNums=fieldL;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int typeL=typeAr.length;

        tdItems=new ArrayList<TDItem>();
        for(int i=0;i<typeL;i++){
           tdItems.add(new TDItem(typeAr[i],"Undefined"));
        }

        fieldNums=typeL;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldNums;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            uuuuuufield
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        TDItem temp=tdItems.get(i);
        return temp.fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        TDItem temp=tdItems.get(i);
        return temp.fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) throw new NoSuchElementException("no such element!");

        for(int i=0;i<fieldNums;i++){
            TDItem temp=tdItems.get(i);
            if (name.equals(temp.fieldName)) {
                return i;
            }
        }
        //throw exception
        throw new NoSuchElementException("no such element!");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (int i = 0; i < fieldNums; i++) {
            Type tempType = this.getFieldType(i);
            totalSize += tempType.getLen();
        }

        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int td1L = td1.numFields();
        int td2L = td2.numFields();

        Type[] tempTypes = new Type[td1L + td2L];
        String[] tempFields = new String[td1L + td2L];

        for (int i = 0; i < td1L; i++) {
            tempFields[i] = td1.getFieldName(i);
            tempTypes[i] = td1.getFieldType(i);
        }

        for (int i = td1L; i < td1L+td2L; i++) {
            tempFields[i] = td2.getFieldName(i-td1L);
            tempTypes[i] = td2.getFieldType(i-td1L);
        }

        return new TupleDesc(tempTypes, tempFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here

        //determine whether o could turn to TupleDesc object
        if(!(o instanceof TupleDesc)) return false;

        //o --> TupleDesc
        TupleDesc tempTupleDesc=(TupleDesc)o;

        // if length of o is not equal to target object, return false
        if (tempTupleDesc.numFields() != this.numFields()) return false;

        // determine every type
        for (int i = 0; i < numFields(); i++){
            if(tempTupleDesc.getFieldType(i) != this.getFieldType(i)) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
