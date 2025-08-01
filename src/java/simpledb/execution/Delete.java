package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private TupleDesc resultTd;
    private boolean hasDeleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.resultTd = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.hasDeleted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return resultTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        hasDeleted = false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (hasDeleted) {
            return null;
        }
        hasDeleted = true;

        int deleteCount = 0;
        BufferPool bufferPool = Database.getBufferPool();

        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                bufferPool.deleteTuple(tid, tuple);
                deleteCount++;
            } catch (IOException err) {
                throw new DbException("Cannot delete tuple:" + err.getMessage());
            }
        }

        Tuple result = new Tuple(resultTd);
        result.setField(0, new IntField(deleteCount));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length != 1) throw new IllegalArgumentException("Expected one child");
        this.child = children[0];
    }

}
