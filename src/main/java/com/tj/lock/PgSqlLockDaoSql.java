package com.tj.lock;

public class PgSqlLockDaoSql extends AbstractLockDaoSql {

    /**
     * The constant INSERT_LOCK_SQL_POSTGRESQL.
     */
    private static final String INSERT_LOCK_SQL_POSTGRESQL = "insert into " + LOCK_TABLE_PLACE_HOLD + "(" + ALL_COLUMNS + ")"
            + " values (?, ?, ?, ?, ?, now(), now())";

    @Override
    public String getInsertLockSQL(String lockTable) {
        return INSERT_LOCK_SQL_POSTGRESQL.replace(LOCK_TABLE_PLACE_HOLD, lockTable);
    }

}
