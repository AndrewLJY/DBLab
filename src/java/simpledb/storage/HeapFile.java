package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    private File f;
    private TupleDesc td;


    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (!(pid instanceof HeapPageId)) {
            throw new IllegalArgumentException("PageId must be a HeapPageId");
        }

        int pageSize = BufferPool.getPageSize();
        int pageNo = pid.getPageNumber();

        //System.out.println("Reading page " + pid.getPageNumber() + " from HeapFile " + getId());

        try (RandomAccessFile raf = new RandomAccessFile(f.getPath(), "r")) {
            //if ((long)(pageNo + 1) * pageSize > raf.length()) {
            if ((long) pageNo * pageSize >= raf.length()) {
                throw new IllegalArgumentException("Page number out of bounds");
            }

            byte[] data = new byte[pageSize];
            raf.seek((long) pageNo * pageSize);
            raf.readFully(data);

            return new HeapPage((HeapPageId) pid, data);

        } catch (IOException e) {
            throw new RuntimeException("Failed to read page: ", e);
        }
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            int pageNum = 0;
            Iterator<Tuple> tupleIter = null;

            public void open() throws DbException, TransactionAbortedException {
                pageNum = 0;
                tupleIter = getTupleIterator(pageNum);
            }

            private Iterator<Tuple> getTupleIterator(int p) throws DbException, TransactionAbortedException {
                HeapPageId pid = new HeapPageId(getId(), p);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                //System.out.println(page);
                return page.iterator();
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
                while ((tupleIter == null || !tupleIter.hasNext()) && pageNum < numPages() - 1) {
                    pageNum++;
                    tupleIter = getTupleIterator(pageNum);
                }
                return tupleIter != null && tupleIter.hasNext();
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();
                return tupleIter.next();
            }

            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            public void close() {
                tupleIter = null;
                pageNum = numPages();
            }

        };
    }

    /*
    public DbFileIterator iterator(final TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int pageIndex = 0;
            private Iterator<Tuple> tupleIterator = null;

            public void open() throws DbException, TransactionAbortedException {
                pageIndex = 0;
                tupleIterator = getTupleIterator(pageIndex);
            }

            private Iterator<Tuple> getTupleIterator(int pageNo) throws DbException, TransactionAbortedException {
                if (pageNo >= numPages()) return null;

                HeapPageId pid = new HeapPageId(getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tupleIterator == null) return false;

                while (!tupleIterator.hasNext() && pageIndex < numPages() - 1) {
                    pageIndex++;
                    tupleIterator = getTupleIterator(pageIndex);
                }
                return tupleIterator != null && tupleIterator.hasNext();
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            public void close() {
                tupleIterator = null;
            }
        };
    }

    */

}

