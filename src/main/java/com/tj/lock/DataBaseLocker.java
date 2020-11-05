package com.tj.lock;


import com.tj.lock.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DataBaseLocker implements Locker {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseLocker.class);
    private static final long TIMEOUT_RETRY_PERIOD = 1000L;
    private static final int TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS = 5000;
    private static final int TIMEOUT_SECONDS = 24 * 60 * 60;
    private final long expire;
    private final LockDao lockDao;

    public DataBaseLocker(DataSource dataSource, Config config) {
        String lockTableName = config.getLockTableName();
        if (StringUtil.isBlank(lockTableName)) {
            throw new LockException("lock table must not be null");
        }
        Long expireSeconds = config.getExpireSeconds();
        if (Objects.isNull(expireSeconds)) {
            expire = TIMEOUT_SECONDS;
        } else if (expireSeconds <= 0) {
            throw new LockException("expire must greater 0");
        } else {
            expire = expireSeconds;
        }
        lockDao = new DataBaseLockDAO(dataSource, lockTableName, config.getDataBaseType());
    }

    private final ScheduledThreadPoolExecutor timeoutSchedule = new ScheduledThreadPoolExecutor(1,
            r -> {
                Thread thread = new Thread(r, "LockTimeoutSchedule");
                thread.setDaemon(true);
                if (thread.getPriority() != Thread.NORM_PRIORITY) {
                    thread.setPriority(Thread.NORM_PRIORITY);
                }
                return thread;
            });

    @Override
    public void init() {
        timeoutSchedule.scheduleAtFixedRate(() -> {
            try {
                timeoutCheck();
            } catch (Exception e) {
                LOGGER.debug("Exception timeout checking ... ", e);
            }
        }, 0, TIMEOUT_RETRY_PERIOD, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryLock(String xid, String tableName, String pk, String rowKey, String lockOwner) {
        LockDO lockDO = convertToLockDo(xid, tableName, pk, rowKey, lockOwner);
        return lockDao.acquireLock(lockDO);
    }

    @Override
    public boolean unLock(String xid, String rowKey, String lockOwner) {
        LockDO lockDO = convertToLockDo(xid, null, null, rowKey, lockOwner);
        return lockDao.unLock(lockDO);
    }

    @Override
    public void destroy() {
        timeoutSchedule.shutdown();
        try {
            timeoutSchedule.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {

        }
    }

    private void timeoutCheck() {
        LocalDateTime time = LocalDateTime.now().minusSeconds(expire);
        lockDao.unLockTimeOut(time);
    }

    private LockDO convertToLockDo(String xid, String tableName, String pk, String rowKey, String lockOwner) {
        LockDO lockDO = new LockDO();
        lockDO.setXid(xid);
        lockDO.setTableName(tableName);
        lockDO.setPk(pk);
        lockDO.setRowKey(rowKey);
        lockDO.setLockOwner(lockOwner);
        return lockDO;
    }
}
