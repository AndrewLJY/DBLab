package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.storage.StringField;
import java.util.*;
import simpledb.storage.Field;
import simpledb.storage.StringField;
import simpledb.storage.IntField;



/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
        private HashMap<Field, Integer> tempAgg;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.tempAgg = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbfieldVal;
         if (this.gbfield != (int) Aggregator.NO_GROUPING) {
            gbfieldVal = tup.getField(this.gbfield);
        } else {
            gbfieldVal = new IntField(this.gbfield);
        }
        String afieldVal = ((StringField) tup.getField(this.afield)).getValue();

            if (!tempAgg.containsKey(gbfieldVal)) {
            tempAgg.put(gbfieldVal, 0);
        }

        tempAgg.put(gbfieldVal, tempAgg.get(gbfieldVal) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc temp;
        List<Tuple> result = new ArrayList<>();
        if(this.gbfield != (int) Aggregator.NO_GROUPING){
            temp = new TupleDesc(new Type[]{this.gbfieldtype,Type.INT_TYPE});
        } else {
            temp = new TupleDesc(new Type[]{Type.INT_TYPE});
        } 

        for(HashMap.Entry<Field, Integer> val: tempAgg.entrySet()){
            Tuple t = new Tuple(temp);
            if(this.gbfield == (int) Aggregator.NO_GROUPING){
                t.setField(0, new IntField(val.getValue()));
            } else {
                t.setField(0, val.getKey());
                t.setField(1, new IntField(val.getValue()));
            }
            result.add(t);
        }

        return new TupleIterator(temp, result);
    }

}
