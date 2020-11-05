package com.tj.lock;

/**
 * the database lock store mysql sql
 */
public class MySqlLockDaoSql extends AbstractLockDaoSql {

    /**
     * The constant INSERT_LOCK_SQL_MYSQL.
     */
    private static final String INSERT_LOCK_SQL_MYSQL = "insert into " + LOCK_TABLE_PLACE_HOLD + "(" + ALL_COLUMNS + ")"
            + " values (?, ?, ?, ?, ?, ?, ?, now(), now())";

    @Override
    public String getInsertLockSQL(String lockTable) {
        return INSERT_LOCK_SQL_MYSQL.replace(LOCK_TABLE_PLACE_HOLD, lockTable);
    }

}
