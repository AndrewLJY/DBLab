package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.storage.IntField;
import java.util.*;
import simpledb.storage.Field;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> tempAgg;
    private HashMap<Field, int[]> avgMap;
    private int count;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        // Can be min, mix, sum, avg, count
        this.what = what;
        this.tempAgg = new HashMap<Field, Integer>();
        this.avgMap = new HashMap<Field, int[]>();
        this.count = 0;

    }

    public int setInitBasedOnOp() {
        switch (this.what) {
            case MIN:
                return Integer.MAX_VALUE;
            case MAX:
                return Integer.MIN_VALUE;
            case SUM:
                return 0;
            case AVG:
            case COUNT:
                return 0;
            default:
                System.out.println("No such operator");
                return -1;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbfieldVal;
        if (this.gbfield != (int)Aggregator.NO_GROUPING) {
            gbfieldVal = tup.getField(this.gbfield);
        } else {
            gbfieldVal = null;
        }
        
        int afieldVal = ((IntField) tup.getField(this.afield)).getValue();

        if (!tempAgg.containsKey(gbfieldVal)) {
            if (this.what == Op.AVG) {
                avgMap.put(gbfieldVal, new int[]{0,0});
            } 
            tempAgg.put(gbfieldVal, this.setInitBasedOnOp());
        }

        switch (this.what) {
            case MIN:
                tempAgg.put(gbfieldVal, Math.min(tempAgg.get(gbfieldVal), afieldVal));
                break;
            case MAX:
                tempAgg.put(gbfieldVal, Math.max(tempAgg.get(gbfieldVal), afieldVal));
                break;
            case SUM:
                tempAgg.put(gbfieldVal, tempAgg.get(gbfieldVal) + afieldVal);
                break;
            case AVG:
                int[] countTrack = avgMap.get(gbfieldVal);
                countTrack[0] += afieldVal;
                countTrack[1]++;
                tempAgg.put(gbfieldVal, (countTrack[0] / countTrack[1]));
                break;
            case COUNT:
                this.count++;
                tempAgg.put(gbfieldVal, tempAgg.get(gbfieldVal) + 1);
                break;
            default:
                System.out.println("No such operator");
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> result = new ArrayList<>();
        TupleDesc temp;
        if (this.gbfield != (int) Aggregator.NO_GROUPING){
            temp = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        } else {
            temp = new TupleDesc(new Type[]{Type.INT_TYPE}); 
        }

        for(Map.Entry<Field, Integer> val : tempAgg.entrySet()){
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
