package com.tj.lock;

public class Config {

    private String lockTableName;
    private Long expireSeconds;
    private DataBaseType dataBaseType;

    public String getLockTableName() {
        return lockTableName;
    }

    public void setLockTableName(String lockTableName) {
        this.lockTableName = lockTableName;
    }

    public Long getExpireSeconds() {
        return expireSeconds;
    }

    public DataBaseType getDataBaseType() {
        return dataBaseType;
    }

    public void setDataBaseType(DataBaseType dataBaseType) {
        this.dataBaseType = dataBaseType;
    }

    public void setExpireSeconds(Long expireSeconds) {
        this.expireSeconds = expireSeconds;
    }
}
