package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private static final long serialVersionUID = 1L;
    private TDItem[] dataAr;
    private HashMap<String, Integer> nameMap;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

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
        Iterable<TDItem> iterableFields = Arrays.asList(this.dataAr);
        return iterableFields.iterator();
    }

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

        this.dataAr = new TDItem[typeAr.length];
        this.nameMap = new HashMap<>();

        for (int i = 0; i < typeAr.length; i++) {
            TDItem newTD = new TDItem(typeAr[i], fieldAr[i]);
            this.dataAr[i] = newTD;
            this.nameMap.put(fieldAr[i], i);
        }
        
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
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.dataAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i > numFields()){
            throw new NoSuchElementException("No such field with index " + i + ".");
        }

        return this.dataAr[i].fieldName;
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
        if(i < 0 || i > this.numFields()){
            throw new NoSuchElementException("No such field with index " + i + ".");
        }

        if (this.dataAr[i] == null) {
            return null;
        }

        return this.dataAr[i].fieldType;
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
        if (this.nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new NoSuchElementException("No such field " + name + ".");
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sum = 0;
        for (TDItem item: this.dataAr){
            sum += item.fieldType.getLen();
        }
        return sum;
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
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] nameAr = new String[td1.numFields() + td2.numFields()];

        for (int i = 0; i < td1.numFields(); i++) {
            typeAr[i] = td1.dataAr[i].fieldType;
            nameAr[i] = td1.dataAr[i].fieldName;
        }

        for (int i = 0; i < td2.numFields(); i++) {
            typeAr[i + td1.numFields()] = td2.dataAr[i].fieldType;
            nameAr[i + td1.numFields()] = td2.dataAr[i].fieldName;
        }
        
        return new TupleDesc(typeAr, nameAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }

        if (!(o instanceof TupleDesc)) {
            return false;
        }

        TupleDesc other = (TupleDesc) o;

        if (other.numFields() != this.numFields()){
            return false;
        }
        
        if(!other.toString().equals(this.toString())){
            return false;
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
        StringBuilder sb = new StringBuilder("");

        for (TDItem eachTD : dataAr) {
            sb.append(eachTD.toString() + ", ");
        }

        sb.delete(sb.length() - 2, sb.length());
    
        return sb.toString(); 
    }
}
