package com.tj.lock;

public interface Locker {

    void init();

    boolean tryLock(String xid, String tableName, String pk, String rowKey, String lockOwner);

    boolean unLock(String xid, String rowKey, String lockOwner);

    void destroy();
}
