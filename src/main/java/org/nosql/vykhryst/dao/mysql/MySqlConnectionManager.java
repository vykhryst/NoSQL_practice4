package org.nosql.vykhryst.dao.mysql;


import org.nosql.vykhryst.util.PropertiesManager;
import org.nosql.vykhryst.util.DBException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySqlConnectionManager {

    private static MySqlConnectionManager instance;
    private static final String URL;
    private static final String USERNAME;
    private static final String PASSWORD;

    static {
        URL = PropertiesManager.getProperty("mysql.database.url");
        USERNAME = PropertiesManager.getProperty("mysql.database.username");
        PASSWORD = PropertiesManager.getProperty("mysql.database.password");
    }

    private MySqlConnectionManager() {
        // Private constructor to prevent instantiation
    }

    public static MySqlConnectionManager getInstance() {
        if (instance == null) {
            instance = new MySqlConnectionManager();
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return getConnection(true);
    }

    public Connection getConnection(boolean autoCommit) throws SQLException {
        Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        connection.setAutoCommit(autoCommit);
        if (!autoCommit) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        return connection;
    }

    public void close(AutoCloseable... resources) throws DBException {
        for (AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    throw new DBException("Can't close resource", e);
                }
            }
        }
    }

    public void rollback(Connection connection) throws DBException {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new DBException("Can't rollback connection", e);
            }
        }
    }

    public void commit(Connection connection) throws DBException {
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                throw new DBException("Can't commit connection", e);
            }
        }
    }
}
