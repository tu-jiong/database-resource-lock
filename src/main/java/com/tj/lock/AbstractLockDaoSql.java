package com.tj.lock;

/**
 * the database abstract lock store sql interface
 */
public class AbstractLockDaoSql implements LockDaoSql {

    /**
     * The constant LOCK_TABLE_PLACE_HOLD.
     */
    protected static final String LOCK_TABLE_PLACE_HOLD = " #lock_table# ";

    /**
     * The constant IN_PARAMS_PLACE_HOLD.
     */
    protected static final String IN_PARAMS_PLACE_HOLD = " #in_params# ";

    /**
     * The constant ALL_COLUMNS.
     * lock_id, table_name, pk, row_key, lock_owner, ctime, utime
     */
    protected static final String ALL_COLUMNS
            = LockTableColumnsName.LOCK_TABLE_XID + ", " + LockTableColumnsName.LOCK_TABLE_TABLE_NAME + ", "
            + LockTableColumnsName.LOCK_TABLE_PK + ", " + LockTableColumnsName.LOCK_TABLE_ROW_KEY + ", "
            + LockTableColumnsName.LOCK_TABLE_LOCK_OWNER + ", " + LockTableColumnsName.LOCK_TABLE_CTIME + ", "
            + LockTableColumnsName.LOCK_TABLE_UTIME;

    /**
     * The constant BATCH_DELETE_LOCK_SQL.
     */
    private static final String DELETE_LOCK_SQL = "delete from " + LOCK_TABLE_PLACE_HOLD
            + " where " + LockTableColumnsName.LOCK_TABLE_XID + " = ? and " + LockTableColumnsName.LOCK_TABLE_ROW_KEY + " = ? "
            + " and " + LockTableColumnsName.LOCK_TABLE_LOCK_OWNER + " = ? ";

    /**
     * The constant QUERY_LOCK_SQL.
     */
    private static final String QUERY_LOCK_SQL = "select " + ALL_COLUMNS + " from " + LOCK_TABLE_PLACE_HOLD
            + " where " + LockTableColumnsName.LOCK_TABLE_ROW_KEY + " = ? ";

    /**
     * The constant CHECK_LOCK_SQL.
     */
    private static final String CHECK_LOCK_SQL = "select " + ALL_COLUMNS + " from " + LOCK_TABLE_PLACE_HOLD
            + " where " + LockTableColumnsName.LOCK_TABLE_ROW_KEY + " in (" + IN_PARAMS_PLACE_HOLD + ")";

    /**
     * The constant DELETE_TIMEOUT_LOCK_SQL.
     */
    private static final String DELETE_TIMEOUT_LOCK_SQL = "delete from " + LOCK_TABLE_PLACE_HOLD
            + " where " + LockTableColumnsName.LOCK_TABLE_CTIME + " < ?";

    @Override
    public String getInsertLockSQL(String lockTable) {
        throw new RuntimeException("can not insert lock");
    }

    @Override
    public String getDeleteLockSql(String lockTable) {
        return DELETE_LOCK_SQL.replace(LOCK_TABLE_PLACE_HOLD, lockTable);
    }

    @Override
    public String getQueryLockSql(String lockTable) {
        return QUERY_LOCK_SQL.replace(LOCK_TABLE_PLACE_HOLD, lockTable);
    }

    @Override
    public String getCheckLockableSql(String lockTable, String paramPlaceHold) {
        return CHECK_LOCK_SQL.replace(LOCK_TABLE_PLACE_HOLD, lockTable).replace(IN_PARAMS_PLACE_HOLD, paramPlaceHold);
    }

    @Override
    public String getDeleteTimeoutLockSql(String lockTable) {
        return DELETE_TIMEOUT_LOCK_SQL.replace(LOCK_TABLE_PLACE_HOLD, lockTable);
    }
}
