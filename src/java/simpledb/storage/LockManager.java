package simpledb.storage;

import simpledb.transaction.TransactionId;
import java.util.*;

public class LockManager {

    enum LockType {
        SHARED,
        EXCLUSIVE
    }

    private static class Lock {
        LockType type;
        Set<TransactionId> currHolders; // Keeps track of what transactions currently holds a lock for current page 

        Lock(LockType type) {
            this.type = type;
            this.currHolders = new HashSet<>();
        }
    }

    private final Map<PageId, Lock> lockMap = new HashMap<>(); // Keeps track of what locks are tied to a given page
    private final Map<TransactionId, Set<PageId>> pagesMap = new HashMap<>(); // Keeps track of what pages a transaction is holding a lock on

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Lock lock = lockMap.get(pid);
        return lock != null && lock.currHolders.contains(tid);
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        Lock lock = lockMap.get(pid);
        if (lock != null) {
            lock.currHolders.remove(tid);
            if (lock.currHolders.isEmpty()) {
                lockMap.remove(pid);
            }
        }

        Set<PageId> pages = pagesMap.get(tid);
        if (pages != null) {
            pages.remove(pid);
            if (pages.isEmpty()) {
                pagesMap.remove(tid);
            }
        }

        notifyAll();
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = pagesMap.get(tid);
        if (pages != null) {
            for (PageId pid : new HashSet<>(pages)) {
                releaseLock(tid, pid);
            }
        }
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType type)
        throws simpledb.transaction.TransactionAbortedException {

        long startTime = System.currentTimeMillis();
        long timeoutDur = 5000;

        while (true) {
            Lock currLock = lockMap.get(pid);

            // New lock
            if (currLock == null) {
                Lock newLock = new Lock(type);
                newLock.currHolders.add(tid);
                lockMap.put(pid, newLock);
                pagesMap.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
                return;
            }

            // Upgrade/downgrade existing lock
            if (type == LockType.SHARED) {
                if (currLock.type == LockType.SHARED ||
                   (currLock.type == LockType.EXCLUSIVE && currLock.currHolders.contains(tid) && currLock.currHolders.size() == 1)) {
                    currLock.currHolders.add(tid);
                    currLock.type = LockType.SHARED;
                    pagesMap.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
                    return;
                }
            } else {
                // LockType.EXCLUSIVE
                if (currLock.currHolders.contains(tid) && currLock.currHolders.size() == 1) {
                    currLock.type = LockType.EXCLUSIVE;
                    return;
                }
            }

            // Deadlock prevention
            try {
                if (System.currentTimeMillis() - startTime > timeoutDur) {
                    throw new simpledb.transaction.TransactionAbortedException();
                }
                wait(100);

            } catch (InterruptedException e) {
                throw new simpledb.transaction.TransactionAbortedException();
            }
        }
    }

}
