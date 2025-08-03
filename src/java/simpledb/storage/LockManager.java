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
    private final Map<TransactionId, Set<PageId>> pagesMap = new HashMap<>(); // Keeps track of what pages a transaction
                                                                              // is holding a lock on
    private final Map<TransactionId, Set<TransactionId>> waitForGraph = new HashMap<>();

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

        removeWaitEdgesPointingTo(tid);
        notifyAll();
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = pagesMap.get(tid);
        if (pages != null) {
            for (PageId pid : new HashSet<>(pages)) {
                releaseLock(tid, pid);
            }
        }

        waitForGraph.remove(tid);
        removeWaitEdgesPointingTo(tid);
        notifyAll();
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType type)
            throws simpledb.transaction.TransactionAbortedException {

        while (true) {
            Lock currLock = lockMap.get(pid);

            // New lock
            if (currLock == null) {
                Lock newLock = new Lock(type);
                newLock.currHolders.add(tid);
                lockMap.put(pid, newLock);
                pagesMap.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
                waitForGraph.remove(tid); // clear wait edges from this txn
                notifyAll();
                return;
            }

            // Upgrade/downgrade existing lock
            if (type == LockType.SHARED) {
                if (currLock.type == LockType.SHARED ||
                        (currLock.type == LockType.EXCLUSIVE && currLock.currHolders.contains(tid)
                                && currLock.currHolders.size() == 1)) {
                    currLock.currHolders.add(tid);
                    currLock.type = LockType.SHARED;
                    pagesMap.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
                    waitForGraph.remove(tid);
                    notifyAll();
                    return;
                }
            } else {
                // LockType.EXCLUSIVE
                if (currLock.currHolders.contains(tid) && currLock.currHolders.size() == 1) {
                    currLock.type = LockType.EXCLUSIVE;
                    notifyAll();
                    return;
                }
            }

            addWaitForEdges(tid, currLock.currHolders);

            // Deadlock prevention
            if (detectCycle()) {
                removeWaitEdgesFrom(tid);
                throw new simpledb.transaction.TransactionAbortedException(); // abort if deadlock detected
            }
            try {
                wait(100);

            } catch (InterruptedException e) {
                throw new simpledb.transaction.TransactionAbortedException();
            }
        }
    }

    private void addWaitForEdges(TransactionId waiter, Set<TransactionId> holders) {
        Set<TransactionId> edges = waitForGraph.computeIfAbsent(waiter, k -> new HashSet<>());

        // Prevent adding an edge to itself
        for (TransactionId holder : holders) {
            if (!holder.equals(waiter)) {
                edges.add(holder);
            }
        }
    }

    /* Remove all edges from tid */
    private void removeWaitEdgesFrom(TransactionId tid) {
        waitForGraph.remove(tid);
    }

    /* Remove all edges that point to tid */
    private void removeWaitEdgesPointingTo(TransactionId tid) {
        for (Set<TransactionId> edges : waitForGraph.values()) {
            edges.remove(tid);
        }
    }

    /* DFS cycle detection */
    private boolean detectCycle() {
        Set<TransactionId> visited = new HashSet<>();
        Set<TransactionId> stack = new HashSet<>();

        for (TransactionId tid : waitForGraph.keySet()) {
            if (dfs(tid, visited, stack)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(TransactionId node, Set<TransactionId> visited, Set<TransactionId> stack) {
        if (stack.contains(node))
            return true;
        if (visited.contains(node))
            return false;

        visited.add(node);
        stack.add(node);

        for (TransactionId neighbor : waitForGraph.getOrDefault(node, Collections.emptySet())) {
            if (dfs(neighbor, visited, stack)) {
                return true;
            }
        }

        stack.remove(node);
        return false;
    }
}