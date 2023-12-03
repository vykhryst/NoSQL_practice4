package org.nosql.vykhryst.dao.mysqlEntityDao;

import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
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

    private final ConnectionManager connectionManager;

    public MySqlCategoryDAO() {
        this.connectionManager = ConnectionManager.getInstance();
    }

    @Override
    public Optional<Category> findById(long id) throws SQLException {
        try (Connection conn = connectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_BY_ID)) {
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? Optional.of(mapCategory(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new DBException("Can't get category by id", e);
        }
    }

    @Override
    public List<Category> findAll() throws SQLException {
        try (Connection conn = connectionManager.getConnection();
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

    private static Category mapCategory(ResultSet rs) throws SQLException {
        Category category = new Category();
        category.setId(rs.getInt(1));
        category.setName(rs.getString(2));
        return category;
    }

    @Override
    public long save(Category category) throws SQLException {
        try (Connection conn = connectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_CATEGORY, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, category.getName());
            stmt.executeUpdate();
            ResultSet programKeys = stmt.getGeneratedKeys();
            if (programKeys.next()) {
                category.setId(programKeys.getInt(1));
            }
            return category.getId();
        } catch (SQLException e) {
            throw new DBException("Can't insert category", e);
        }
    }

    @Override
    public boolean update(Category entity) throws SQLException {
        try(Connection conn = connectionManager.getConnection();
            PreparedStatement stmt = conn.prepareStatement(UPDATE_CATEGORY)) {
            stmt.setString(1, entity.getName());
            stmt.setLong(2, entity.getId());
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't update category", e);
        }
    }

    @Override
    public boolean delete(long id) throws SQLException {
        try (Connection conn = connectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(DELETE_BY_ID)) {
            stmt.setLong(1, id);
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't delete category", e);
        }
    }
}
