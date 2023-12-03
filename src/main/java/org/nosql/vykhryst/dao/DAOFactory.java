package org.nosql.vykhryst.dao;

import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;

public interface DAOFactory {
    AdvertisingDAO getAdvertisingDAO();
    ClientDAO getClientDAO();
    ProgramDAO getProgramDAO();

    CategoryDAO getCategoryDAO();
}
