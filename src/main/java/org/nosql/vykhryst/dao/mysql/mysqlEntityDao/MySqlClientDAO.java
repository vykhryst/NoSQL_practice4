package org.nosql.vykhryst.dao.mysql.mysqlEntityDao;


import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.mysql.MySqlConnectionManager;
import org.nosql.vykhryst.entity.Client;
import org.nosql.vykhryst.util.DBException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MySqlClientDAO implements ClientDAO {

    private static final String SELECT_ALL_CLIENTS = "SELECT * FROM client";
    private static final String SELECT_CLIENT_BY_ID = "SELECT * FROM client WHERE id = ?";
    public static final String SELECT_CLIENT_BY_USERNAME = "SELECT * FROM client WHERE username = ?";
    private static final String INSERT_CLIENT = "INSERT INTO client (username, firstname, lastname, phone_number, email, password) VALUES (?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_CLIENT = "UPDATE client SET username = ?, firstname = ?, lastname = ?, phone_number = ?, email = ?, password = ? WHERE id = ?";
    private static final String DELETE_CLIENT_BY_ID = "DELETE FROM client WHERE id = ?";
    public static final String SELECT_BY_EMAIL_AND_PASSWORD = "SELECT * FROM client WHERE email = ? AND password = ?";
    private final MySqlConnectionManager mySqlConnectionManager;

    public MySqlClientDAO() {
        this.mySqlConnectionManager = MySqlConnectionManager.getInstance();
    }

    @Override
    public List<Client> findAll() {
        try (Connection conn = mySqlConnectionManager.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_ALL_CLIENTS)) {
            List<Client> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapClient(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new DBException("Can't get all clients", e);
        }
    }

    @Override
    public Optional<Client> findById(String id) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_CLIENT_BY_ID)) {
            stmt.setLong(1, Long.parseLong(id));
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.of(mapClient(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DBException("Can't get client by id", e);
        }
    }

    @Override
    public String save(Client client) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_CLIENT, Statement.RETURN_GENERATED_KEYS)) {
            setClientStatement(client, stmt);
            stmt.executeUpdate();
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                if (keys.next()) {
                    client.setId(String.valueOf(keys.getLong(1)));
                }
            }
            return client.getId();
        } catch (SQLException e) {
            throw new DBException("Can't insert client", e);
        }
    }

    @Override
    public boolean update(Client client) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(UPDATE_CLIENT)) {
            setClientStatement(client, stmt);
            stmt.setLong(7, Long.parseLong(client.getId()));
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't update client", e);
        }

    }

    @Override
    public boolean delete(String id) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(DELETE_CLIENT_BY_ID)) {
            stmt.setLong(1, Long.parseLong(id));
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't delete client", e);
        }
    }

    @Override
    public Optional<Client> findByUsername(String username) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_CLIENT_BY_USERNAME)) {
            stmt.setString(1, username);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.of(mapClient(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DBException("Can't get client by username", e);
        }
    }

    public List<Client> findByEmailAndPassword(String email, String password) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_BY_EMAIL_AND_PASSWORD)) {
            stmt.setString(1, email);
            stmt.setString(2, password);
            try (ResultSet rs = stmt.executeQuery()) {
                List<Client> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapClient(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new DBException("Can't get client by email and password", e);
        }
    }

    @Override
    public long deleteClientAndPrograms(long id) {
        long result = -1; // Default failure value
        try (Connection conn = mySqlConnectionManager.getConnection();
             CallableStatement stmt = conn.prepareCall("{call delete_client_and_programs(?)}")) {
            stmt.setLong(1, id);
            stmt.executeUpdate();
            // Retrieving the result from the stored procedure
            ResultSet rs = stmt.getResultSet();
            if (rs != null && rs.next()) {
                result = rs.getLong("Result");
            }
            return result;
        } catch (SQLException e) {
            throw new DBException("Can't delete client and programs", e);
        }
    }


    private Client mapClient(ResultSet rs) throws SQLException {
        return new Client.Builder().id(String.valueOf(rs.getInt("id")))
                .username(rs.getString("username"))
                .firstname(rs.getString("firstname"))
                .lastname(rs.getString("lastname"))
                .phoneNumber(rs.getString("phone_number"))
                .email(rs.getString("email"))
                .password(rs.getString("password"))
                .build();
    }

    private static void setClientStatement(Client client, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, client.getUsername());
        stmt.setString(2, client.getFirstname());
        stmt.setString(3, client.getLastname());
        stmt.setString(4, client.getPhoneNumber());
        stmt.setString(5, client.getEmail());
        stmt.setString(6, client.getPassword());
    }
}
