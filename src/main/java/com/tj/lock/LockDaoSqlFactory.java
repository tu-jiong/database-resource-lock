package com.tj.lock;

/**
 * the database lock store factory
 */
public class LockDaoSqlFactory {
    /**
     * get the lock dao sql
     *
     * @return lock dao sql
     */
    public static LockDaoSql getLogStoreSql(DataBaseType dataBaseType) {
        switch (dataBaseType) {
            case MySql:
                return new MySqlLockDaoSql();
            case PgSql:
                return new PgSqlLockDaoSql();
            default:
                throw new LockException("not support database type");
        }
    }

}
