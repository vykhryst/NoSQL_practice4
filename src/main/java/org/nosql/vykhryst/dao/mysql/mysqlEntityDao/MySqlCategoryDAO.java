package org.nosql.vykhryst.dao.mysql.mysqlEntityDao;

import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.dao.mysql.MySqlConnectionManager;
import org.nosql.vykhryst.entity.Category;
import org.nosql.vykhryst.util.DBException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MySqlCategoryDAO implements CategoryDAO {
    private static final String SELECT_ALL = "SELECT id, name FROM category";
    private static final String INSERT_CATEGORY = "INSERT INTO category (name) VALUES (?)";
    private static final String DELETE_BY_ID = "DELETE FROM category WHERE id = ?";
    public static final String SELECT_BY_ID = "SELECT id, name FROM category WHERE id = ?";
    public static final String UPDATE_CATEGORY = "UPDATE category SET name = ? WHERE id = ?";

    private final MySqlConnectionManager mySqlConnectionManager;

    public MySqlCategoryDAO() {
        this.mySqlConnectionManager = MySqlConnectionManager.getInstance();
    }

    @Override
    public Optional<Category> findById(String id) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_BY_ID)) {
            stmt.setLong(1, Long.parseLong(id));
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.of(mapCategory(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DBException("Can't get category by id", e);
        }
    }

    @Override
    public List<Category> findAll() {
        try (Connection conn = mySqlConnectionManager.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_ALL)) {
            List<Category> result = new ArrayList<>();
            while (rs.next()) {
                Category category = mapCategory(rs);
                result.add(category);
            }
            return result;
        } catch (SQLException e) {
            throw new DBException("Can't get all categories", e);
        }
    }

    @Override
    public String save(Category category) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_CATEGORY, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, category.getName());
            stmt.executeUpdate();
            ResultSet programKeys = stmt.getGeneratedKeys();
            if (programKeys.next()) {
                category.setId(String.valueOf(programKeys.getInt(1)));
            }
            return category.getId();
        } catch (SQLException e) {
            throw new DBException("Can't insert category", e);
        }
    }

    @Override
    public boolean update(Category entity) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(UPDATE_CATEGORY)) {
            stmt.setString(1, entity.getName());
            stmt.setLong(2, Long.parseLong(entity.getId()));
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't update category", e);
        }
    }

    @Override
    public boolean delete(String id) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(DELETE_BY_ID)) {
            stmt.setLong(1, Long.parseLong(id));
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't delete category", e);
        }
    }

    @Override
    public Category findByName(String name) {
        try (Connection conn = mySqlConnectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_ALL + " WHERE name = ?")) {
            stmt.setString(1, name);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? mapCategory(rs) : null;
            }
        } catch (SQLException e) {
            throw new DBException("Can't get category by name", e);
        }
    }

    private static Category mapCategory(ResultSet rs) throws SQLException {
        Category category = new Category();
        category.setId(String.valueOf(rs.getInt(1)));
        category.setName(rs.getString(2));
        return category;
    }
}
