package org.nosql.vykhryst;

import org.nosql.vykhryst.dao.DaoFactory;
import org.nosql.vykhryst.dao.TypeDAO;
import org.nosql.vykhryst.dao.entityDao.*;

public class DataMigrator {
    private final DaoFactory daoFactory;

    public DataMigrator() {
        this.daoFactory = DaoFactory.getInstance();
    }

    public void migrateData(TypeDAO sourceType, TypeDAO destinationType) {
        migrateCategoryDAO(sourceType, destinationType);
        migrateAdvertisingDAO(sourceType, destinationType);
        migrateClientDAO(sourceType, destinationType);
        migrateProgramDAO(sourceType, destinationType);
    }

    private void migrateCategoryDAO(TypeDAO sourceType, TypeDAO destinationType) {
        CategoryDAO sourceDAO = daoFactory.getCategoryDAO(sourceType);
        CategoryDAO destinationDAO = daoFactory.getCategoryDAO(destinationType);
        sourceDAO.findAll().forEach(destinationDAO::save);
    }

    private void migrateAdvertisingDAO(TypeDAO sourceType, TypeDAO destinationType) {
        AdvertisingDAO sourceDAO = daoFactory.getAdvertisingDAO(sourceType);
        AdvertisingDAO destinationDAO = daoFactory.getAdvertisingDAO(destinationType);
        sourceDAO.findAll().forEach(destinationDAO::migrate);
    }

    private void migrateClientDAO(TypeDAO sourceType, TypeDAO destinationType) {
        ClientDAO sourceDAO = daoFactory.getClientDAO(sourceType);
        ClientDAO destinationDAO = daoFactory.getClientDAO(destinationType);
        sourceDAO.findAll().forEach(destinationDAO::save);
    }

    private void migrateProgramDAO(TypeDAO sourceType, TypeDAO destinationType) {
        ProgramDAO sourceDAO = daoFactory.getProgramDAO(sourceType);
        ProgramDAO destinationDAO = daoFactory.getProgramDAO(destinationType);
        sourceDAO.findAll().forEach(destinationDAO::migrate);
    }

    public static void main(String[] args) {
        DataMigrator dataMigrator = new DataMigrator();
//        dataMigrator.migrateData(TypeDAO.MYSQL, TypeDAO.MONGODB);

        // For migration in the opposite direction, use:
        dataMigrator.migrateData(TypeDAO.MONGODB, TypeDAO.MYSQL);
    }
}
