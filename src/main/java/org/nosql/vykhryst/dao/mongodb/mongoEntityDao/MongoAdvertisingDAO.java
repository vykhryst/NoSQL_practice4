package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.mongodb.MongoConnectionManager;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Category;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.time.ZoneOffset.UTC;

public class MongoAdvertisingDAO implements AdvertisingDAO {

    private final MongoCollection<Document> advertisingCollection;

    public MongoAdvertisingDAO() {
        this.advertisingCollection = MongoConnectionManager.getCollection("advertising");
    }

    @Override
    public Optional<Advertising> findById(String id) {
        Document query = createIdQuery(id);
        Document result = advertisingCollection.find(query).first();
        return result != null ? Optional.of(mapDocumentToAdvertising(result)) : Optional.empty();
    }


    @Override
    public List<Advertising> findAll() {
        List<Advertising> advertisingList = new ArrayList<>();
        for (Document doc : advertisingCollection.find()) {
            advertisingList.add(mapDocumentToAdvertising(doc));
        }
        return advertisingList;
    }

    @Override
    public String save(Advertising advertising) {
        Document doc = mapAdvertisingToDocument(advertising);
        advertisingCollection.insertOne(doc);
        advertising.setId(doc.getObjectId("_id").toString());
        return advertising.getId();
    }

    @Override
    public boolean update(Advertising entity) {
        Document query = createIdQuery(entity.getId());
        Document doc = mapAdvertisingToDocument(entity);
        return advertisingCollection.updateOne(query, new Document("$set", doc)).wasAcknowledged();
    }


    @Override
    public boolean delete(String id) {
        Document query = createIdQuery(id);
        return advertisingCollection.deleteOne(query).wasAcknowledged();
    }

    @Override
    public Optional<Advertising> findByName(String name) {
        Document query = new Document("name", name);
        Document result = advertisingCollection.find(query).first();
        return result != null ? Optional.of(mapDocumentToAdvertising(result)) : Optional.empty();
    }

    @Override
    public String migrate(Advertising advertising) {
        Document doc = new Document("name", advertising.getName())
                .append("description", advertising.getDescription())
                .append("measurement", advertising.getMeasurement())
                .append("unitPrice", new Decimal128(advertising.getUnitPrice()))
                .append("updatedAt", advertising.getUpdatedAt());
        Category category = findCategoryByName(advertising.getCategory().getName());
        doc.append("category", new Document("_id", new ObjectId(category.getId()))
                .append("name", category.getName()));
        advertisingCollection.insertOne(doc);
        advertising.setId(doc.getObjectId("_id").toString());
        return advertising.getId();
    }

    private Category findCategoryByName(String name) {
        Document query = new Document("name", name);
        Document categoryDoc = MongoConnectionManager.getCollection("category").find(query).first();
        return categoryDoc != null ? mapDocumentToCategory(categoryDoc) : null;
    }

    public List<Advertising> findByNameAndUnitPrice(String name, BigDecimal unitPrice) {
        Document query = new Document();
        if (name != null) {
            query.append("name", name);
        }
        if (unitPrice != null) {
            query.append("unitPrice", new Decimal128(unitPrice));
        }
        List<Advertising> advertisingList = new ArrayList<>();
        for (Document doc : advertisingCollection.find(query)) {
            advertisingList.add(mapDocumentToAdvertising(doc));
        }
        return advertisingList;
    }


    // Utility methods
    private Advertising mapDocumentToAdvertising(Document result) {
        return new Advertising.Builder()
                .id(result.getObjectId("_id").toString())
                .name(result.getString("name"))
                .description(result.getString("description"))
                .measurement(result.getString("measurement"))
                .unitPrice(result.get("unitPrice", Decimal128.class).bigDecimalValue())
                .updatedAt(result.getDate("updatedAt").toInstant().atZone(UTC).toLocalDateTime())
                .category(mapDocumentToCategory(result.get("category", Document.class)))
                .build();
    }

    private static Category mapDocumentToCategory(Document categoryDoc) {
        return new Category(categoryDoc.getObjectId("_id").toString(), categoryDoc.getString("name"));
    }

    private static Document mapAdvertisingToDocument(Advertising entity) {
        return new Document("name", entity.getName())
                .append("description", entity.getDescription())
                .append("measurement", entity.getMeasurement())
                .append("unitPrice", new Decimal128(entity.getUnitPrice()))
                .append("updatedAt", entity.getUpdatedAt())
                .append("category", new Document("_id", new ObjectId(entity.getCategory().getId()))
                        .append("name", entity.getCategory().getName()));
    }

    private Document createIdQuery(String id) {
        return new Document("_id", new ObjectId(id));
    }
}
