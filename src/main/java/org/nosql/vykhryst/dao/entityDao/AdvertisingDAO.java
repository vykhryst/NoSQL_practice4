package org.nosql.vykhryst.dao.entityDao;


import org.nosql.vykhryst.dao.DAO;
import org.nosql.vykhryst.entity.Advertising;

import java.sql.SQLException;
import java.util.Optional;

public interface AdvertisingDAO extends DAO<Advertising> {
    Optional<Advertising> findByName(String name) throws SQLException;
}
