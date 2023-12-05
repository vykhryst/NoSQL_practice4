package org.nosql.vykhryst.dao.entityDao;


import org.nosql.vykhryst.dao.DAO;
import org.nosql.vykhryst.entity.Client;

import java.util.List;
import java.util.Optional;

public interface ClientDAO extends DAO<Client> {
    Optional<Client> findByUsername(String username);
    long deleteClientAndPrograms(long id);
    public List<Client> findByEmailAndPassword(String email, String password);
}
