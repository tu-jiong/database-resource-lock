package com.tj.lock;

import java.time.LocalDateTime;
import java.util.List;

/**
 * The interface Lock store.
 */
public interface LockDao {

    /**
     * Acquire lock boolean.
     *
     * @param lockDO the lock do
     * @return the boolean
     */
    boolean acquireLock(LockDO lockDO);


    /**
     * Acquire lock boolean.
     *
     * @param lockDOs the lock d os
     * @return the boolean
     */
    boolean acquireLock(List<LockDO> lockDOs);

    /**
     * Un lock boolean.
     *
     * @param lockDO the lock do
     * @return the boolean
     */
    boolean unLock(LockDO lockDO);

    /**
     * Un lock boolean.
     *
     * @param lockDOs the lock d os
     * @return the boolean
     */
    boolean unLock(List<LockDO> lockDOs);

    /**
     * Is lockable boolean.
     *
     * @param lockDOs the lock do
     * @return the boolean
     */
    boolean isLockable(List<LockDO> lockDOs);

    /**
     * release timout lock
     *
     * @param time specify dead line
     * @return the boolean
     */
    boolean unLockTimeOut(LocalDateTime time);
}
