package org.nosql.vykhryst.dao;

import java.util.List;
import java.util.Optional;

public interface DAO<T> {

    Optional<T> findById(String id);

    List<T> findAll();

    String save(T entity);

    boolean update(T entity);

    boolean delete(String id);
}
