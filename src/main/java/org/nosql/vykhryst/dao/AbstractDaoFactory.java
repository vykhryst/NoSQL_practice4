package org.nosql.vykhryst.dao;

import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;

public interface AbstractDaoFactory {
    AdvertisingDAO getAdvertisingDAO(TypeDAO type);

    ClientDAO getClientDAO(TypeDAO type);

    ProgramDAO getProgramDAO(TypeDAO type);

    CategoryDAO getCategoryDAO(TypeDAO type);
}
