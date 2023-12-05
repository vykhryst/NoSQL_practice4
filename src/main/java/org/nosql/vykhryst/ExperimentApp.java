package org.nosql.vykhryst;

import org.nosql.vykhryst.dao.AbstractDaoFactory;
import org.nosql.vykhryst.dao.TypeDAO;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.DaoFactory;
import org.nosql.vykhryst.entity.Client;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class ExperimentApp {
    public static void main(String[] args) {
        // Initialize DAO Factory
        AbstractDaoFactory daoFactory = DaoFactory.getInstance();
        ClientDAO mongoClientDAO = daoFactory.getClientDAO(TypeDAO.MONGODB);
        ClientDAO mySqlClientDAO = daoFactory.getClientDAO(TypeDAO.MYSQL);

        int count = 500000;

        // Test MongoDB
        System.out.println("\n--- Test MongoDB ---");
        testSave(mongoClientDAO, count);
        testFindAll(mongoClientDAO);

        // Test MySQL
        System.out.println("\n--- Test MySQL ---");
        testSave(mySqlClientDAO, count);
        testFindAll(mySqlClientDAO);
    }


    private static void testSave(ClientDAO clientDAO, int count) {
        // Insert data
        Instant start = Instant.now();
        for (int i = 0; i < count; i++) {
            Client client = new Client();
            client.setUsername("username" + i);
            client.setFirstname("firstname" + i);
            client.setLastname("lastname" + i);
            client.setPhoneNumber("tel" + i);
            client.setEmail("email" + i);
            client.setPassword("password" + i);
            clientDAO.save(client);
        }
        Instant end = Instant.now();
        System.out.println("Insertion time for " + count + " records: "
                + Duration.between(start, end).toMillis() + " ms");
    }

    private static void testFindAll(ClientDAO clientDAO) {
        // Find all data
        Instant start = Instant.now();
        List<Client> clients = clientDAO.findAll();
        Instant end = Instant.now();
        System.out.println("Reading time for " + clients.size() + " records: "
                + Duration.between(start, end).toMillis() + " ms");
    }
}
