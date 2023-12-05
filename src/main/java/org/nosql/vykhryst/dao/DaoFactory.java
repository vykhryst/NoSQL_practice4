package org.nosql.vykhryst.dao;

import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;
import org.nosql.vykhryst.dao.mongodb.mongoEntityDao.MongoAdvertisingDAO;
import org.nosql.vykhryst.dao.mongodb.mongoEntityDao.MongoCategoryDAO;
import org.nosql.vykhryst.dao.mongodb.mongoEntityDao.MongoClientDAO;
import org.nosql.vykhryst.dao.mongodb.mongoEntityDao.MongoProgramDAO;
import org.nosql.vykhryst.dao.mysql.mysqlEntityDao.MySqlAdvertisingDAO;
import org.nosql.vykhryst.dao.mysql.mysqlEntityDao.MySqlCategoryDAO;
import org.nosql.vykhryst.dao.mysql.mysqlEntityDao.MySqlClientDAO;
import org.nosql.vykhryst.dao.mysql.mysqlEntityDao.MySqlProgramDAO;

public class DaoFactory implements AbstractDaoFactory {

    private static DaoFactory instance = new DaoFactory();

    private DaoFactory() {
        // Приватний конструктор, щоб заборонити створення екземплярів
    }

    public static DaoFactory getInstance() {
        if (instance == null) {
            instance = new DaoFactory();
        }
        return instance;
    }

    public AdvertisingDAO getAdvertisingDAO(TypeDAO type) {
        if (type == TypeDAO.MYSQL) {
            return new MySqlAdvertisingDAO();
        } else if (type == TypeDAO.MONGODB) {
            return new MongoAdvertisingDAO();
        }
        return null;
    }


    public ClientDAO getClientDAO(TypeDAO type) {
        if (type == TypeDAO.MYSQL) {
            return new MySqlClientDAO();
        } else if (type == TypeDAO.MONGODB) {
            return new MongoClientDAO();
        }
        return null;
    }

    public ProgramDAO getProgramDAO(TypeDAO type) {
        if (type == TypeDAO.MYSQL) {
            return new MySqlProgramDAO();
        } else if (type == TypeDAO.MONGODB) {
            return new MongoProgramDAO();
        }
        return null;
    }

    public CategoryDAO getCategoryDAO(TypeDAO type) {
        if (type == TypeDAO.MYSQL) {
            return new MySqlCategoryDAO();
        } else if (type == TypeDAO.MONGODB) {
            return new MongoCategoryDAO();
        }
        return null;
    }

}
