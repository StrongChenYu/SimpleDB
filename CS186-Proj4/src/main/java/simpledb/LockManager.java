package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private final ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    private final ConcurrentHashMap<PageId, HashSet<TransactionId>> shareLocks;

    public LockManager() {
        exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
        shareLocks = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
    }

    /*
    * 锁的五个原则
    * 1. 读之前加share锁
    * 2. 写之前加exclusive锁
    * 3. 多个读锁可以共享
    * 4. 写锁只有在这个页上没有读锁的情况下可以加
    * 5. 当一个页面只有一个读锁时，这个读锁可以升级为写锁
    * */

    //检测是否可以授予锁
    //等于1为写请求
    //等于0为读请求
    public synchronized boolean grantLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {
        if (perm.permLevel == 0) return grantSLock(pid, tid);
        if (perm.permLevel == 1) return grantXLock(pid, tid);
        throw new TransactionAbortedException();
    }

    /*
     * 赋予读锁主要检测
     * 1.这个页有没有写锁，没有true
     * 1.2.这个页的写锁是不是自己加的 是true,否 false
     * */
    private boolean grantSLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        TransactionId tidLock = exclusiveLocks.get(pid);
        if (tidLock == null) {
            lockPage(pid, tid, Permissions.READ_ONLY);
            return true;
        } else {
            if (tidLock.equals(tid)) {
                //如果是自己加的，降级为读锁
                releaseLock(pid, tid);
                lockPage(pid, tid, Permissions.READ_ONLY);
                return true;
            } else {
                return false;
            }
        }
    }

    /*
    * 赋予写锁主要检测
    * 1. 这个页是否有写锁：没有转到2，有：判断写锁是不是自己的？：是返回true，不是返回false
    * 2. 这个页是否有读锁
    * 2.1 这个页的读锁是否只有一个，并且还是自己加的
    * */
    private boolean grantXLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        TransactionId tidWriteLock = exclusiveLocks.get(pid);
        HashSet<TransactionId> tidReadLocks = shareLocks.get(pid);

        if (tidWriteLock == null) {
            if (tidReadLocks == null || tidReadLocks.size() == 0) {
                //没有读锁也没有写锁
                lockPage(pid, tid ,Permissions.READ_WRITE);
                return true;
            }
            else {
                //有读锁且不止一个
                if (tidReadLocks.size() > 1) return false;
                //只有一个读锁
                else if (tidReadLocks.contains(tid)){
                    //读锁的事务就是。该事务，可以升级为写锁
                    releaseLock(pid, tid);
                    lockPage(pid, tid, Permissions.READ_WRITE);
                    return true;
                    //仅有的读锁不是该事务的读锁
                } else return false;
            }
        } else {
            //判断写锁是不是这个事务的
            return tidWriteLock.equals(tid);
        }
    }

    //锁页
    private synchronized void lockPage(PageId pid, TransactionId tid, Permissions perm) {
        //加写锁
        if (perm.permLevel == 1) exclusiveLocks.put(pid, tid);
        //加读锁
        else if (perm.permLevel == 0) {
            HashSet<TransactionId> shareLock = shareLocks.get(pid);
            if (shareLock == null) {
                shareLock = new HashSet<TransactionId>();
                shareLock.add(tid);
                shareLocks.put(pid, shareLock);
            } else shareLock.add(tid);
        }
    }

    //释放锁
    public synchronized boolean releaseLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        Permissions perm = holdLockType(pid, tid);
        if (perm.permLevel == 0){
            HashSet<TransactionId> shareLock = shareLocks.get(pid);
            if (shareLock == null) return false;
            if (shareLock.contains(tid)) {
                //判断释放的是否是自己的锁
                shareLock.remove(tid);
                return true;
            } else return false;
        }
        else if (perm.permLevel == 1){
            TransactionId writeTid = exclusiveLocks.get(pid);
            if (writeTid == null) return false;
            else if (writeTid.equals(tid)) {
                //判断是否是属于自己的锁
                exclusiveLocks.remove(pid);
                return true;
            } else {
                return false;
            }
        }
        throw new TransactionAbortedException();
    }

    //判断页面上锁的类型,0为读锁，1为写锁
    public Permissions holdLockType(PageId pageId, TransactionId tid) {
        TransactionId writeTid = exclusiveLocks.get(pageId);
        HashSet<TransactionId> readTids = shareLocks.get(pageId);

        if (writeTid != null) return Permissions.READ_WRITE;
        if (readTids != null && readTids.size() > 0) return Permissions.READ_ONLY;

        //null is incorrect
        return null;
    }

    public synchronized void deadLockDetection() {

    }

    public static void main(String[] args) {
        HashSet<Integer> set = new HashSet<Integer>();
        set.remove(1);
    }

}
