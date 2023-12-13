package org.nosql.vykhryst.testApps;

import org.nosql.vykhryst.dao.AbstractDaoFactory;
import org.nosql.vykhryst.dao.DaoFactory;
import org.nosql.vykhryst.dao.TypeDAO;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.mongodb.mongoEntityDao.MongoClientDAO;
import org.nosql.vykhryst.entity.Client;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class TestReplicaApp {
    public static void main(String[] args) {

        // Initialize DAO Factory
        AbstractDaoFactory daoFactory = DaoFactory.getInstance();

        ClientDAO mongoClientDAO = daoFactory.getClientDAO(TypeDAO.MONGODB);

        int count = 10000;

        System.out.println("\n--- Test INSERT ---");
        testInsert((MongoClientDAO) mongoClientDAO, count);

        System.out.println("\n--- Test SELECT ---");
        testSelect(mongoClientDAO);

    }

    private static void testInsert(MongoClientDAO clientDAO, int count) {
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
            clientDAO.insertWithReplica(client);
        }
        Instant end = Instant.now();
        System.out.println("Insertion time for " + count + " records: "
                + Duration.between(start, end).toMillis() + " ms");
    }

    private static void testSelect(ClientDAO clientDAO) {
        // Find all data
        Instant start = Instant.now();
        List<Client> clients = clientDAO.findAll();
        Instant end = Instant.now();
        System.out.println("Reading time for " + clients.size() + " records: "
                + Duration.between(start, end).toMillis() + " ms");
    }
}
