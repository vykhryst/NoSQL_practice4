package org.nosql.vykhryst.dao.mysqlEntityDao;


import org.nosql.vykhryst.dao.entityDao.ProgramDAO;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Category;
import org.nosql.vykhryst.entity.Client;
import org.nosql.vykhryst.entity.Program;
import org.nosql.vykhryst.util.DBException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MySqlProgramDAO implements ProgramDAO {

    private static final String INSERT_PROGRAM = "INSERT INTO program (client_id, campaign_title, description) VALUES (?, ?, ?);";
    private static final String DELETE_PROGRAM_BY_ID = "DELETE FROM program WHERE id = ?";
    private static final String INSERT_PROGRAM_ADVERTISING = "INSERT INTO program_advertising (program_id, advertising_id, quantity) VALUES (?, ?, ?)";
    private static final String UPDATE_PROGRAM_QUANTITY = "UPDATE program_advertising SET quantity = ? WHERE program_id = ? AND advertising_id = ?";
    private static final String DELETE_PROGRAM_ADVERTISING = "DELETE FROM program_advertising WHERE program_id = ? AND advertising_id = ?";
    public static final String SELECT_ALL_PROGRAMS = "SELECT p.id, c.id,c.username,c.firstname, c.lastname, c.phone_number, c.email, c.password, p.campaign_title,p.description, p.created_at\n" +
            "FROM program p  INNER JOIN client c ON p.client_id = c.id;";
    public static final String SELECT_PROGRAM_ADVERTISING = "SELECT a.id, c.id, c.name, a.name, a.measurement, a.unit_price, a.description, a.updated_at, pa.quantity\n" +
            "FROM program p INNER JOIN program_advertising pa ON p.id = pa.program_id INNER JOIN advertising a ON pa.advertising_id = a.id   INNER JOIN category c on a.category_id = c.id WHERE p.id = ?;";
    public static final String SELECT_PROGRAM_BY_ID = "SELECT p.id,c.id,c.username,c.firstname,c.lastname,c.phone_number,c.email,c.password,p.campaign_title,p.description,p.created_at\n" +
            "FROM program p INNER JOIN client c ON p.client_id = c.id WHERE p.id = ?";
    private final ConnectionManager connectionManager;

    public MySqlProgramDAO() {
        this.connectionManager = ConnectionManager.getInstance();
    }

    @Override
    public List<Program> findAll() throws DBException {
        try (Connection conn = connectionManager.getConnection();
             Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery(SELECT_ALL_PROGRAMS);
            List<Program> programs = new ArrayList<>();
            while (rs.next()) {
                Program program = mapProgram(rs);
                programs.add(program);
            }
            getProgramAdvertisings(conn, programs);
            return programs;
        } catch (SQLException e) {
            throw new DBException(e);
        }
    }

    private static Program mapProgram(ResultSet rs) throws SQLException {
        return new Program.Builder()
                .id(rs.getInt("id"))
                .client(new Client.Builder().id(rs.getInt("c.id"))
                        .username(rs.getString("c.username"))
                        .firstname(rs.getString("c.firstname"))
                        .lastname(rs.getString("c.lastname"))
                        .phoneNumber(rs.getString("c.phone_number"))
                        .email(rs.getString("c.email"))
                        .password(rs.getString("c.password"))
                        .build())
                .campaignTitle(rs.getString("campaign_title"))
                .description(rs.getString("description"))
                .createdAt(rs.getTimestamp("created_at").toLocalDateTime())
                .build();
    }

    private static void getProgramAdvertisings(Connection conn, List<Program> programs) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement(SELECT_PROGRAM_ADVERTISING)) {
            for (Program program : programs) {
                st.setLong(1, program.getId());
                ResultSet rs2 = st.executeQuery();
                while (rs2.next()) {
                    Advertising programAdvertising = mapProgramAdvertising(rs2);
                    program.addAdvertising(programAdvertising, rs2.getInt("pa.quantity"));
                }
            }
        }
    }

    private static Advertising mapProgramAdvertising(ResultSet rs2) throws SQLException {
        return new Advertising.Builder()
                .id(rs2.getInt("a.id"))
                .category(new Category(rs2.getInt("c.id"), rs2.getString("c.name")))
                .name(rs2.getString("a.name"))
                .measurement(rs2.getString("a.measurement"))
                .unitPrice(rs2.getBigDecimal("a.unit_price"))
                .description(rs2.getString("a.description"))
                .updatedAt(rs2.getTimestamp("a.updated_at").toLocalDateTime())
                .build();
    }

    @Override
    public Optional<Program> findById(long id) throws DBException {
        try (Connection conn = connectionManager.getConnection();
             PreparedStatement st = conn.prepareStatement(SELECT_PROGRAM_BY_ID)) {
            st.setLong(1, id);
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    Program program = mapProgram(rs);
                    getProgramAdvertisings(conn, List.of(program));
                    return Optional.of(program);
                }
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DBException(e.getMessage(), e);
        }
    }

    @Override
    public long save(Program program) throws DBException {
        Connection conn = null;
        PreparedStatement st = null;
        try {
            conn = connectionManager.getConnection(false); // false - no auto commit
            // insert program
            st = conn.prepareStatement(INSERT_PROGRAM, Statement.RETURN_GENERATED_KEYS);
            st.setLong(1, program.getClient().getId());
            st.setString(2, program.getCampaignTitle());
            st.setString(3, program.getDescription());
            st.executeUpdate();
            // get generated id
            ResultSet programKeys = st.getGeneratedKeys();
            // insert program advertisings
            st = conn.prepareStatement(INSERT_PROGRAM_ADVERTISING);
            if (programKeys.next()) {
                program.setId(programKeys.getLong(1));
                for (Map.Entry<Advertising, Integer> entry : program.getAdvertising().entrySet()) {
                    st.setLong(1, program.getId());
                    st.setLong(2, entry.getKey().getId());
                    st.setInt(3, entry.getValue());
                    st.addBatch();
                }
                st.executeBatch();
            }
            connectionManager.commit(conn); // commit transaction
            return program.getId();
        } catch (SQLException e) {
            connectionManager.rollback(conn); // rollback transaction
            throw new DBException(e.getMessage(), e);
        } finally {
            connectionManager.close(conn, st); // close connection and statement
        }
    }

    @Override
    public boolean saveAdvertisingToProgram(long programId, Map<Advertising, Integer> advertising) throws DBException {
        Connection conn = null;
        PreparedStatement st = null;
        try {
            conn = connectionManager.getConnection(false); // false - no auto commit
            // insert program advertisings
            st = conn.prepareStatement(INSERT_PROGRAM_ADVERTISING);
            // iterate over program advertisings
            for (Map.Entry<Advertising, Integer> entry : advertising.entrySet()) {
                st.setLong(1, programId);
                st.setLong(2, entry.getKey().getId());
                st.setInt(3, entry.getValue());
                st.addBatch();
            }
            st.executeBatch();
            connectionManager.commit(conn); // commit transaction
            return true;
        } catch (SQLException e) {
            connectionManager.rollback(conn); // rollback transaction
            throw new DBException(e.getMessage());
        } finally {
            connectionManager.close(conn, st); // close connection and statement
        }
    }

    @Override
    public boolean update(Program program) throws DBException {
        try (Connection conn = connectionManager.getConnection();
             PreparedStatement st = conn.prepareStatement(UPDATE_PROGRAM_QUANTITY)) {
            for (Map.Entry<Advertising, Integer> programAdvertising : program.getAdvertising().entrySet()) {
                st.setInt(1, programAdvertising.getValue());
                st.setLong(2, program.getId());
                st.setLong(3, programAdvertising.getKey().getId());
                st.addBatch();
            }
            return st.executeBatch().length > 0;
        } catch (SQLException e) {
            throw new DBException("Can't update program", e);
        }
    }

    @Override
    public boolean delete(long id) throws DBException {
        try (Connection conn = connectionManager.getConnection();
             PreparedStatement st = conn.prepareStatement(DELETE_PROGRAM_BY_ID)) {
            st.setLong(1, id);
            return st.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't delete program", e);
        }
    }

    @Override
    public boolean deleteAdvertisingFromProgram(long programId, long advertisingId) throws DBException {
        try (Connection conn = connectionManager.getConnection();
             PreparedStatement st = conn.prepareStatement(DELETE_PROGRAM_ADVERTISING)) {
            st.setLong(1, programId);
            st.setLong(2, advertisingId);
            return st.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DBException("Can't delete advertising from program", e);
        }
    }
}
