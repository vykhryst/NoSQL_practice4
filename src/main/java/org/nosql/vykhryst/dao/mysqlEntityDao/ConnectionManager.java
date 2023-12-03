package org.nosql.vykhryst.dao.mysqlEntityDao;


import org.nosql.vykhryst.util.DBException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {

    private static ConnectionManager instance;
    private static final String URL;
    private static final String USERNAME;
    private static final String PASSWORD;
    private static final String DATABASE_PROPERTIES = "database/database.properties";

    static {
        Properties properties = loadProperties();
        URL = properties.getProperty("mysql.database.url");
        USERNAME = properties.getProperty("mysql.database.username");
        PASSWORD = properties.getProperty("mysql.database.password");
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = ConnectionManager.class.getClassLoader().getResourceAsStream(DATABASE_PROPERTIES)) {
            if (input == null) {
                throw new RuntimeException("Sorry, unable to find " + DATABASE_PROPERTIES);
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file", e);
        }
        return properties;
    }

    private ConnectionManager() {
        // Private constructor to prevent instantiation
    }

    public static ConnectionManager getInstance() {
        if (instance == null) {
            instance = new ConnectionManager();
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
