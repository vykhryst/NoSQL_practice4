package org.nosql.vykhryst.dao.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.nosql.vykhryst.util.PropertiesManager;

public class MongoConnectionManager {

    private static final String CONNECTION_URL;
    private static final String DATABASE_NAME;

    private static final MongoDatabase DATABASE;

    private MongoConnectionManager() {}

    static {
        CONNECTION_URL = PropertiesManager.getProperty("mongo.connection.url");
        DATABASE_NAME = PropertiesManager.getProperty("mongo.database.name");

        MongoClient mongoClient = MongoClients.create(CONNECTION_URL);
        DATABASE = mongoClient.getDatabase(DATABASE_NAME);
    }

    public static MongoCollection<Document> getCollection(String collection) {
        return DATABASE.getCollection(collection);
    }
}