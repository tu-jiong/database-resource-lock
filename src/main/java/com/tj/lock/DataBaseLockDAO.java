package com.tj.lock;

import com.tj.lock.utils.CollectionUtil;
import com.tj.lock.utils.IOUtil;
import com.tj.lock.utils.LambdaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The type Data base lock store.
 */
public class DataBaseLockDAO implements LockDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseLockDAO.class);

    /**
     * The Lock store data source.
     */
    protected DataSource dataSource;

    /**
     * The Lock table.
     */
    protected String lockTable;
    /**
     * The database type
     */
    protected DataBaseType dataBaseType;

    /**
     * Instantiates a new Data base lock store dao.
     *
     * @param dataSource the log store data source
     */
    public DataBaseLockDAO(DataSource dataSource, String lockTable, DataBaseType dataBaseType) {
        this.lockTable = lockTable;
        this.dataSource = dataSource;
        this.dataBaseType = dataBaseType;
        if (dataBaseType == null) {
            this.dataBaseType = DataBaseType.MySql;
        }
        if (dataSource == null) {
            throw new LockException("lockStoreDataSource can not null");
        }
    }

    /**
     * Sets lock table.
     *
     * @param lockTable the lock table
     */
    public void setLockTable(String lockTable) {
        this.lockTable = lockTable;
    }

    /**
     * Sets log store data source.
     *
     * @param dataSource the log store data source
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public boolean acquireLock(LockDO lockDO) {
        return acquireLock(Collections.singletonList(lockDO));
    }

    @Override
    public boolean acquireLock(List<LockDO> lockDOs) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Set<String> dbExistedRowKeys = new HashSet<>();
        boolean originalAutoCommit = true;
        if (lockDOs.size() > 1) {
            lockDOs = lockDOs.stream().filter(LambdaUtil.distinctByKey(LockDO::getRowKey)).collect(Collectors.toList());
        }
        try {
            conn = dataSource.getConnection();
            if (originalAutoCommit = conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
            //check lock
            StringJoiner sj = new StringJoiner(",");
            for (int i = 0; i < lockDOs.size(); i++) {
                sj.add("?");
            }
            boolean canLock = true;
            //query
            String checkLockSQL = LockDaoSqlFactory.getLogStoreSql(dataBaseType).getCheckLockableSql(lockTable, sj.toString());
            ps = conn.prepareStatement(checkLockSQL);
            for (int i = 0; i < lockDOs.size(); i++) {
                ps.setString(i + 1, lockDOs.get(i).getRowKey());
            }
            rs = ps.executeQuery();
            String currentXID = lockDOs.get(0).getXid();
            while (rs.next()) {
                String dbXID = rs.getString(LockTableColumnsName.LOCK_TABLE_XID);
                if (!Objects.equals(dbXID, currentXID)) {
                    if (LOGGER.isDebugEnabled()) {
                        String dbPk = rs.getString(LockTableColumnsName.LOCK_TABLE_PK);
                        String dbTableName = rs.getString(LockTableColumnsName.LOCK_TABLE_TABLE_NAME);
                        Long dbLockOwner = rs.getLong(LockTableColumnsName.LOCK_TABLE_LOCK_OWNER);
                        LOGGER.debug("lock on [{}:{}] is holding by xid {} lockOwner {}", dbTableName, dbPk, dbXID,
                                dbLockOwner);
                    }
                    canLock = false;
                    break;
                }
                dbExistedRowKeys.add(rs.getString(LockTableColumnsName.LOCK_TABLE_ROW_KEY));
            }

            if (!canLock) {
                conn.rollback();
                return false;
            }
            List<LockDO> unrepeatedLockDOs;
            if (!CollectionUtil.isEmpty(dbExistedRowKeys)) {
                unrepeatedLockDOs = lockDOs.stream().filter(lockDO -> !dbExistedRowKeys.contains(lockDO.getRowKey()))
                        .collect(Collectors.toList());
            } else {
                unrepeatedLockDOs = lockDOs;
            }
            if (CollectionUtil.isEmpty(unrepeatedLockDOs)) {
                conn.rollback();
                return true;
            }
            //lock
            if (unrepeatedLockDOs.size() == 1) {
                LockDO lockDO = unrepeatedLockDOs.get(0);
                if (!doAcquireLock(conn, lockDO)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("lock acquire failed, xid {} lockOwner {} pk {}", lockDO.getXid(), lockDO.getLockOwner(), lockDO.getPk());
                    }
                    conn.rollback();
                    return false;
                }
            } else {
                if (!doAcquireLocks(conn, unrepeatedLockDOs)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("lock batch acquire failed, xid {} lockOwner {} pks {}", unrepeatedLockDOs.get(0).getXid(),
                                unrepeatedLockDOs.get(0).getLockOwner(), unrepeatedLockDOs.stream().map(LockDO::getPk).collect(Collectors.toList()));
                    }
                    conn.rollback();
                    return false;
                }
            }
            conn.commit();
            return true;
        } catch (SQLException e) {
            throw new LockException(e);
        } finally {
            IOUtil.close(rs, ps);
            if (conn != null) {
                try {
                    if (originalAutoCommit) {
                        conn.setAutoCommit(true);
                    }
                    conn.close();
                } catch (SQLException e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public boolean unLock(LockDO lockDO) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(true);
            //release lock
            String deleteLockSql = LockDaoSqlFactory.getLogStoreSql(dataBaseType).getDeleteLockSql(lockTable);
            ps = conn.prepareStatement(deleteLockSql);
            ps.setString(1, lockDO.getXid());
            ps.setString(2, lockDO.getRowKey());
            ps.setString(3, lockDO.getLockOwner());
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            throw new LockException(e);
        } finally {
            IOUtil.close(ps, conn);
        }
    }

    @Override
    public boolean unLock(List<LockDO> lockDOs) {
        Connection conn = null;
        PreparedStatement ps = null;
        boolean originalAutoCommit = true;
        try {
            conn = dataSource.getConnection();
            if (originalAutoCommit = conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
            String deleteLockSql = LockDaoSqlFactory.getLogStoreSql(dataBaseType).getDeleteLockSql(lockTable);
            ps = conn.prepareStatement(deleteLockSql);
            for (int i = 0; i < lockDOs.size(); i++) {
                LockDO lockDO = lockDOs.get(0);
                ps.setString(1, lockDO.getXid());
                ps.setString(2, lockDO.getRowKey());
                ps.setString(3, lockDO.getLockOwner());
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            return true;
        } catch (SQLException e) {
            throw new LockException(e);
        } finally {
            IOUtil.close(ps);
            if (conn != null) {
                try {
                    if (originalAutoCommit) {
                        conn.setAutoCommit(true);
                    }
                    conn.close();
                } catch (SQLException e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public boolean isLockable(List<LockDO> lockDOs) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(true);
            if (!checkLockable(conn, lockDOs)) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new LockException(e);
        } finally {
            IOUtil.close(conn);
        }
    }

    /**
     * Do acquire lock boolean.
     *
     * @param conn   the conn
     * @param lockDO the lock do
     * @return the boolean
     */
    protected boolean doAcquireLock(Connection conn, LockDO lockDO) {
        PreparedStatement ps = null;
        try {
            //insert
            String insertLockSQL = LockDaoSqlFactory.getLogStoreSql(dataBaseType).getInsertLockSQL(lockTable);
            ps = conn.prepareStatement(insertLockSQL);
            ps.setString(1, lockDO.getXid());
            ps.setString(2, lockDO.getTableName());
            ps.setString(3, lockDO.getPk());
            ps.setString(4, lockDO.getRowKey());
            ps.setString(5, lockDO.getLockOwner());
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            LOGGER.error("lock acquire error: {}", e.getMessage(), e);
            //return false,let the caller go to conn.rollabck()
            return false;
        } finally {
            IOUtil.close(ps);
        }
    }

    /**
     * Do acquire lock boolean.
     *
     * @param conn    the conn
     * @param lockDOs the lock do list
     * @return the boolean
     */
    protected boolean doAcquireLocks(Connection conn, List<LockDO> lockDOs) {
        PreparedStatement ps = null;
        try {
            //insert
            String insertLockSQL = LockDaoSqlFactory.getLogStoreSql(dataBaseType).getInsertLockSQL(lockTable);
            ps = conn.prepareStatement(insertLockSQL);
            for (LockDO lockDO : lockDOs) {
                ps.setString(1, lockDO.getXid());
                ps.setString(2, lockDO.getTableName());
                ps.setString(3, lockDO.getPk());
                ps.setString(4, lockDO.getRowKey());
                ps.setString(5, lockDO.getLockOwner());
                ps.addBatch();
            }
            return ps.executeBatch().length == lockDOs.size();
        } catch (SQLException e) {
            LOGGER.error("lock batch acquire error: {}", e.getMessage(), e);
            //return false,let the caller go to conn.rollabck()
            return false;
        } finally {
            IOUtil.close(ps);
        }
    }

    /**
     * Check lock boolean.
     *
     * @param conn    the conn
     * @param lockDOs the lock do
     * @return the boolean
     */
    protected boolean checkLockable(Connection conn, List<LockDO> lockDOs) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            StringJoiner sj = new StringJoiner(",");
            for (int i = 0; i < lockDOs.size(); i++) {
                sj.add("?");
            }

            //query
            String checkLockSQL = LockDaoSqlFactory.getLogStoreSql(dataBaseType).getCheckLockableSql(lockTable, sj.toString());
            ps = conn.prepareStatement(checkLockSQL);
            for (int i = 0; i < lockDOs.size(); i++) {
                ps.setString(i + 1, lockDOs.get(i).getRowKey());
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                String xid = rs.getString(LockTableColumnsName.LOCK_TABLE_XID);
                if (!Objects.equals(xid, lockDOs.get(0).getXid())) {
                    return false;
                }
            }
            return true;
        } catch (SQLException e) {
            throw new LockException(e);
        } finally {
            IOUtil.close(rs, ps);
        }
    }

    @Override
    public boolean unLockTimeOut(LocalDateTime time) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(true);
            ps = conn.prepareStatement(LockDaoSqlFactory.getLogStoreSql(dataBaseType).getDeleteTimeoutLockSql(lockTable));
            ZoneId zone = ZoneId.systemDefault();
            Instant instant = time.atZone(zone).toInstant();
            ps.setDate(1, new Date(instant.toEpochMilli()));
            ps.executeUpdate();
            return true;
        } catch (Exception e) {
            LOGGER.error("time out release fail {}", e.getMessage());
            return false;
        } finally {
            IOUtil.close(rs, ps, conn);
        }
    }
}
