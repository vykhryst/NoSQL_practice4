package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.entity.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MongoClientDAO implements ClientDAO {
    private final MongoCollection<Document> clientCollection;

    public MongoClientDAO() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("advertising_agency");
        this.clientCollection = database.getCollection("client");
    }

    @Override
    public Optional<Client> findById(String id) {
        Document query = createIdQuery(id);
        Document client = clientCollection.find(query).first();
        return client != null ? Optional.of(mapDocumentToClient(client)) : Optional.empty();
    }

    @Override
    public List<Client> findAll() {
        List<Client> clients = new ArrayList<>();
        for (Document doc : clientCollection.find()) {
            clients.add(mapDocumentToClient(doc));
        }
        return clients;
    }

    @Override
    public String save(Client entity) {
        Document doc = mapClientToDocument(entity);
        clientCollection.insertOne(doc);
        entity.setId(doc.getObjectId("_id").toString());
        return entity.getId();
    }

    @Override
    public boolean update(Client entity) {
        Document query = createIdQuery(entity.getId());
        Document doc = mapClientToDocument(entity);
        return clientCollection.updateOne(query, new Document("$set", doc)).wasAcknowledged();
    }

    @Override
    public boolean delete(String id) {
        Document query = createIdQuery(id);
        return clientCollection.deleteOne(query).wasAcknowledged();
    }

    @Override
    public Optional<Client> findByUsername(String username) {
        Document query = new Document("username", username);
        Document client = clientCollection.find(query).first();
        return client != null ? Optional.of(mapDocumentToClient(client)) : Optional.empty();
    }

    public List<Client> findByEmailAndPassword(String email, String password) {
        List<Client> clients = new ArrayList<>();
        Document query = new Document("email", email).append("password", password);
        for (Document doc : clientCollection.find(query)) {
            clients.add(mapDocumentToClient(doc));
        }
        return clients;
    }


    @Override
    public long deleteClientAndPrograms(long id) {
        return 0;
    }

    // Utility methods
    private Document createIdQuery(String id) {
        return new Document("_id", new ObjectId(id));
    }

    private Client mapDocumentToClient(Document client) {
        return new Client.Builder()
                .id(client.getObjectId("_id").toString())
                .username(client.getString("username"))
                .password(client.getString("password"))
                .email(client.getString("email"))
                .firstname(client.getString("firstname"))
                .lastname(client.getString("lastname"))
                .phoneNumber(client.getString("phoneNumber"))
                .build();
    }

    private static Document mapClientToDocument(Client entity) {
        return new Document("username", entity.getUsername())
                .append("firstname", entity.getFirstname())
                .append("lastname", entity.getLastname())
                .append("phoneNumber", entity.getPhoneNumber())
                .append("email", entity.getEmail())
                .append("password", entity.getPassword());
    }
}
