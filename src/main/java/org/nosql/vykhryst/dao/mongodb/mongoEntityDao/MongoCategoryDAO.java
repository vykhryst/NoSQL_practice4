package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.entity.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MongoCategoryDAO implements CategoryDAO {

    private final MongoCollection<Document> categoryCollection;

    public MongoCategoryDAO() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("advertising_agency");
        this.categoryCollection = database.getCollection("category");
    }

    @Override
    public Optional<Category> findById(String id) {
        Document query = createIdQuery(id);
        Document result = categoryCollection.find(query).first();
        return result != null ? Optional.of(mapDocumentToCategory(result)) : Optional.empty();
    }

    @Override
    public List<Category> findAll() {
        List<Category> categories = new ArrayList<>();
        for (Document doc : categoryCollection.find()) {
            categories.add(mapDocumentToCategory(doc));
        }
        return categories;
    }

    @Override
    public String save(Category entity) {
        Document doc = new Document("name", entity.getName());
        categoryCollection.insertOne(doc);
        entity.setId(doc.getObjectId("_id").toString());
        return entity.getId();
    }

    @Override
    public boolean update(Category entity) {
        Document query = createIdQuery(entity.getId());
        Document doc = new Document("name", entity.getName());
        return categoryCollection.updateOne(query, new Document("$set", doc)).wasAcknowledged();
    }

    @Override
    public boolean delete(String id) {
        Document query = createIdQuery(id);
        return categoryCollection.deleteOne(query).wasAcknowledged();
    }

    private static Category mapDocumentToCategory(Document doc) {
        return new Category(doc.getObjectId("_id").toString(), doc.getString("name"));
    }
    private static Document createIdQuery(String id) {
        return new Document("_id", new ObjectId(id));
    }
}
