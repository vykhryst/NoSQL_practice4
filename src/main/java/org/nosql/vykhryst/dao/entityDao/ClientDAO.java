package org.nosql.vykhryst.dao.entityDao;


import org.nosql.vykhryst.dao.DAO;
import org.nosql.vykhryst.entity.Client;
import org.nosql.vykhryst.util.DBException;

import java.util.Optional;

public interface ClientDAO extends DAO<Client> {
    Optional<Client> findByUsername(String username) throws DBException;
    long deleteClientAndPrograms(long id) throws DBException;
}
