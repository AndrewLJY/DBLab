package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private LinkedHashMap<PageId, Page> pages;

    private final LockManager lockManager = new LockManager();
    private LockManager.LockType lockType;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new LinkedHashMap<PageId, Page>(numPages, 0.75f, true);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        if (perm == Permissions.READ_ONLY) {
            lockType = LockManager.LockType.SHARED;
        } else {
            lockType = LockManager.LockType.EXCLUSIVE;
        }

        lockManager.acquireLock(tid, pid, lockType);

        // Check if page is cached in buffer pool
        if (pages.containsKey(pid)) {
            return pages.get(pid);
        }

        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());

        // Get Page
        Page retrievedPage = dbFile.readPage(pid);

        if (pages.size() < numPages) {
            pages.put(pid, retrievedPage);
            return retrievedPage;
        } else {
            // Buffer pool full, evict or throw (not implemented yet)
            // throw new DbException("buffer pool full");
            evictPage();
            pages.put(pid, retrievedPage);
            return retrievedPage;
        }

    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

        Set<Map.Entry<PageId, Page>> entrySet = pages.entrySet();
        for (Map.Entry<PageId, Page> eachEntry : entrySet) {

            PageId pid = eachEntry.getKey();
            Page page = eachEntry.getValue();

            if (holdsLock(tid, pid) && page.isDirty() != null) {
                if (commit) {
                    try {
                        flushPage(pid);
                    } catch (IOException err) {
                        err.printStackTrace();
                    }
                } else {
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page retrievedPage = dbFile.readPage(pid);

                    pages.put(pid, retrievedPage);
                }
            }

        }
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);

        // Insert & get list of dirty pages
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);

        for (Page eachPage : dirtyPages) {
            eachPage.markDirty(true, tid);
            pages.put(eachPage.getId(), eachPage); // cache
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);

        // Delete & get list of dirty pages
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);

        for (Page eachPage : dirtyPages) {
            eachPage.markDirty(true, tid);
            pages.put(eachPage.getId(), eachPage); // cache
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        List<PageId> pageIds = new ArrayList<>(pages.keySet());
        for (PageId eachPid : pageIds) {
            flushPage(eachPid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pages.get(pid);
        if (page != null && page.isDirty() != null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> eachEntry : pages.entrySet()) {
            PageId pid = eachEntry.getKey();
            Page page = eachEntry.getValue();

            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Set<Map.Entry<PageId, Page>> entrySet = pages.entrySet();
        for (Map.Entry<PageId, Page> eachEntry : entrySet) {

            PageId pid = eachEntry.getKey();
            Page page = eachEntry.getValue();

            if (page.isDirty() == null) {
                try {
                    flushPage(pid);
                } catch (IOException err) {
                    throw new DbException("Unable to flush page:" + err.getMessage());
                }
                discardPage(pid);
                return;
            }
        }
        throw new DbException("All pages are dirty, unable to evict.");
    }

}