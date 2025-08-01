package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.execution.OpIterator;


import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate jp;

    private OpIterator child1;

    private OpIterator child2;

    private HashMap<String, Boolean> alrdInNotEquals;

    private HashMap<String, Boolean> alrdInEquals;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.jp = p;
        this.child1 = child1;
        this.child2 = child2;
        if(this.jp.getOperator() != Predicate.Op.EQUALS){
            this.alrdInNotEquals = new HashMap<String, Boolean>();
        }else{
            this.alrdInEquals = new HashMap<String, Boolean>();
        }
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.jp;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return Integer.toString(this.jp.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return Integer.toString(this.jp.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc1 = this.child1.getTupleDesc();
        TupleDesc tupleDesc2 = this.child2.getTupleDesc();
        TupleDesc mergeTupleDesc = TupleDesc.merge(tupleDesc1, tupleDesc2);
        return mergeTupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.child1.open();
        this.child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child1.close();
        this.child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child1.rewind();
        this.child2.rewind();
        if(this.jp.getOperator() == Predicate.Op.EQUALS){
            this.alrdInEquals.clear();
        }

    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!this.child1.hasNext() || !this.child2.hasNext()) return null;
        while(this.child1.hasNext()){
            Tuple outerT = this.child1.next();
            while(this.child2.hasNext()){
                Tuple innerT = this.child2.next();
                if(this.jp.filter(outerT, innerT)){
                    TupleDesc outerTD = outerT.getTupleDesc();
                    TupleDesc innerTD = innerT.getTupleDesc();
                    TupleDesc mergedTD = TupleDesc.merge(outerTD, innerTD);
                    Tuple resultT = new Tuple(mergedTD);

                    int fieldCount1 = outerTD.numFields();
                    int fieldCount2 = innerTD.numFields();

                    for(int x=0; x<fieldCount1; x++){
                        resultT.setField(x, outerT.getField(x));
                    }
                    for(int x=fieldCount1; x< fieldCount1+fieldCount2; x++){
                        resultT.setField(x, innerT.getField(x-fieldCount1));
                    }

                    if(this.jp.getOperator() != Predicate.Op.EQUALS){
                        if(!this.alrdInNotEquals.containsKey(resultT.toString())){
                            this.alrdInNotEquals.put(resultT.toString(), true);
                        }else{
                            continue;
                        }
                    }else{
                        if(!this.alrdInEquals.containsKey(resultT.toString())){
                            this.alrdInEquals.put(resultT.toString(), true);
                            if(this.child2.hasNext()){
                                this.child1.rewind();
                            }else{
                                this.child2.rewind();
                            }
                        }else{
                            continue;
                        }
                    }

                    return resultT;
                }
            }
            this.child2.rewind();
            
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
