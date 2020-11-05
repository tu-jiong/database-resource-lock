package com.tj.lock;

/**
 * the database lock store sql interface
 */
public interface LockDaoSql {

    /**
     * Get insert lock sql string.
     *
     * @param lockTable the lock table
     * @return the string
     */
    String getInsertLockSQL(String lockTable);

    /**
     * Get batch delete lock sql string.
     *
     * @param lockTable the lock table
     * @return the string
     */
    String getDeleteLockSql(String lockTable);

    /**
     * Get query lock sql string.
     *
     * @param lockTable the lock table
     * @return the string
     */
    String getQueryLockSql(String lockTable);

    /**
     * Get check lock sql string.
     *
     * @param lockTable      the lock table
     * @param paramPlaceHold the param place hold
     * @return the string
     */
    String getCheckLockableSql(String lockTable, String paramPlaceHold);

    /**
     * Get timeout lock
     *
     * @param lockTable the lock table
     * @return
     */
    String getDeleteTimeoutLockSql(String lockTable);

}
