package com.tj.test;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.tj.lock.Config;
import com.tj.lock.DataBaseLocker;
import com.tj.lock.LockTableColumnsName;
import com.tj.lock.Locker;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Example {

    @Test
    public void testLock() throws SQLException {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName("localhost");
        dataSource.setDatabaseName("user");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        dataSource.setPort(3306);
        dataSource.setServerTimezone("");

        Config config = new Config();
        config.setLockTableName("lock_table");
        config.setExpireSeconds(24 * 60 * 60L);
        Locker locker = new DataBaseLocker(dataSource, config);

        //application start
        locker.init();

        String xid = "xid";//锁的业务id
        String tableName = "resourceTableName";//锁定的资源表
        String pk = "resourcePk";//锁定资源主键
        String rowKey = "rowKey";//锁的主键，可根据业务生成，例如需要锁定的资源MD5
        String lockOwner = "lockOwner";//锁的持有者
        //获得锁
        locker.tryLock(xid, tableName, pk, rowKey, lockOwner);
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(true);
        PreparedStatement ps = connection.prepareStatement("select * from lock_table");
        ResultSet rs1 = ps.executeQuery();
        while (rs1.next()) {
            Assert.assertEquals(xid, rs1.getString(LockTableColumnsName.LOCK_TABLE_XID));
            System.out.println(rs1.getString(LockTableColumnsName.LOCK_TABLE_XID));
            Assert.assertEquals(rowKey, rs1.getString(LockTableColumnsName.LOCK_TABLE_ROW_KEY));
            System.out.println(rs1.getString(LockTableColumnsName.LOCK_TABLE_ROW_KEY));
            Assert.assertEquals(lockOwner, rs1.getString(LockTableColumnsName.LOCK_TABLE_LOCK_OWNER));
            System.out.println(rs1.getString(LockTableColumnsName.LOCK_TABLE_LOCK_OWNER));
        }

        //释放锁
        locker.unLock(xid, rowKey, lockOwner);

        ResultSet rs2 = ps.executeQuery("select count(*) as count from lock_table");
        while (rs2.next()) {
            int count = rs2.getInt("count");
            Assert.assertEquals(0, count);
        }

        //application stop
        locker.destroy();
    }
}
