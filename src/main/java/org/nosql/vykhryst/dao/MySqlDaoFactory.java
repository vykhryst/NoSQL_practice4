package org.nosql.vykhryst.dao;

import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;
import org.nosql.vykhryst.dao.mysqlEntityDao.MySqlAdvertisingDAO;
import org.nosql.vykhryst.dao.mysqlEntityDao.MySqlCategoryDAO;
import org.nosql.vykhryst.dao.mysqlEntityDao.MySqlClientDAO;
import org.nosql.vykhryst.dao.mysqlEntityDao.MySqlProgramDAO;

public class MySqlDaoFactory implements DAOFactory {

    private static MySqlDaoFactory instance = new MySqlDaoFactory();

    private MySqlDaoFactory() {
        // Приватний конструктор, щоб заборонити створення екземплярів
    }

    public static MySqlDaoFactory getInstance() {
        if (instance == null) {
            instance = new MySqlDaoFactory();
        }
        return instance;
    }

    public AdvertisingDAO getAdvertisingDAO() {
        return new MySqlAdvertisingDAO();
    }

    public ClientDAO getClientDAO() {
        return new MySqlClientDAO();
    }

    public ProgramDAO getProgramDAO() {
        return new MySqlProgramDAO();
    }

    public CategoryDAO getCategoryDAO() {
        return new MySqlCategoryDAO();
    }

}
