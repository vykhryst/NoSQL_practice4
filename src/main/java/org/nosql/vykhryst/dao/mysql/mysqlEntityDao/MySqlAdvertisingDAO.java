package org.nosql.vykhryst.dao.mysql.mysqlEntityDao;


import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.mysql.MySqlConnectionManager;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Category;
import org.nosql.vykhryst.util.DBException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MySqlAdvertisingDAO implements AdvertisingDAO {
    private static final String SELECT_ALL_AD = "SELECT a.id, c.id, c.name, a.name, a.measurement, a.unit_price, a.description, a.updated_at FROM advertising a LEFT JOIN category c ON a.category_id = c.id";
    private static final String SELECT_AD_BY_ID = "SELECT a.id, c.id, c.name, a.name, a.measurement, a.unit_price, a.description, a.updated_at  FROM advertising a LEFT JOIN category c ON a.category_id = c.id WHERE a.id = ?;";
    private static final String SELECT_AD_BY_NAME = "SELECT a.id, c.id, c.name, a.name, a.measurement, a.unit_price, a.description, a.updated_at  FROM advertising a LEFT JOIN category c ON a.category_id = c.id WHERE a.name = ?;";
    private static final String INSERT_AD = "INSERT INTO advertising (category_id, name, measurement, unit_price, description) VALUES (?, ?, ?, ?, ?)";
    private static final String UPDATE_AD = "UPDATE advertising SET category_id = ?, name = ?, measurement = ?, unit_price = ?, description = ? WHERE id = ?";
    private static final String DELETE_AD_BY_ID = "DELETE FROM advertising WHERE id = ?";
    private final MySqlConnectionManager mySqlConnectionManager;

    public MySqlAdvertisingDAO() {
        this.mySqlConnectionManager = MySqlConnectionManager.getInstance();
    }

    @Override
    public List<Advertising> findAll() {
        try (Connection conn = mySqlConnectionManager.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_ALL_AD)) {
            List<Advertising> result = new ArrayList<>();
            while (rs.next()) {
                Advertising advertising = mapAdvertising(rs);
                result.add(advertising);
            }
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException("Can't get all advertising", e);
        }
    }

    @Override
    public Optional<Advertising> findById(String id) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_AD_BY_ID)) {
            stmt.setLong(1, Long.parseLong(id));
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.of(mapAdvertising(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DBException("Can't get advertising by id", e);
        }
    }

    @Override
    public String save(Advertising advertising) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_AD, Statement.RETURN_GENERATED_KEYS)) {
            setStatement(advertising, stmt);
            stmt.executeUpdate();
            ResultSet programKeys = stmt.getGeneratedKeys();
            if (programKeys.next()) {
                advertising.setId(String.valueOf(programKeys.getLong(1)));
            }
            return advertising.getId();
        } catch (SQLException e) {
            throw new DBException("Can't insert advertising", e);
        }
    }


    @Override
    public boolean update(Advertising advertising) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(UPDATE_AD)) {
            setStatement(advertising, stmt);
            stmt.setLong(6, Long.parseLong(advertising.getId()));
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't update advertising", e);
        }
    }

    @Override
    public boolean delete(String id) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(DELETE_AD_BY_ID)) {
            stmt.setLong(1, Long.parseLong(id));
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException("Can't delete advertising", e);
        }
    }

    @Override
    public Optional<Advertising> findByName(String name) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_AD_BY_NAME)) {
            stmt.setString(1, name);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.of(mapAdvertising(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DBException("Can't get advertising by name", e);
        }
    }

    private static Advertising mapAdvertising(ResultSet rs) throws SQLException {
        return new Advertising.Builder()
                .id(Long.toString(rs.getInt("a.id")))
                .name(rs.getString("a.name"))
                .measurement(rs.getString("a.measurement"))
                .unitPrice(rs.getBigDecimal("a.unit_price"))
                .description(rs.getString("a.description"))
                .category(new Category(Long.toString(rs.getInt("c.id")), rs.getString("c.name")))
                .updatedAt(rs.getTimestamp("a.updated_at").toLocalDateTime())
                .build();
    }

    private static void setStatement(Advertising advertising, PreparedStatement stmt) throws SQLException {
        stmt.setLong(1, Long.parseLong(advertising.getCategory().getId()));
        stmt.setString(2, advertising.getName());
        stmt.setString(3, advertising.getMeasurement());
        stmt.setBigDecimal(4, advertising.getUnitPrice());
        stmt.setString(5, advertising.getDescription());
    }
}
