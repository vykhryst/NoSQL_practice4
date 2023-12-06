package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.dao.mongodb.MongoConnectionManager;
import org.nosql.vykhryst.entity.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MongoCategoryDAO implements CategoryDAO {

    private final MongoCollection<Document> categoryCollection;

    public MongoCategoryDAO() {
        this.categoryCollection = MongoConnectionManager.getCollection("category");
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

    @Override
    public Category findByName(String name) {
        Document query = new Document("name", name);
        Document result = categoryCollection.find(query).first();
        return result != null ? mapDocumentToCategory(result) : null;
    }
}
