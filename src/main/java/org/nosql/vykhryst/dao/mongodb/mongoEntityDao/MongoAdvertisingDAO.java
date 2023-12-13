package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.mongodb.MongoConnectionManager;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Category;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
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


    // Запит 1: Отримання кількості рекламних об'єктів у кожній категорії
    public Map<String, Integer> aggregateCountAdsPerCategory() {
        Map<String, Integer> result = new HashMap<>();
        advertisingCollection
                .aggregate(List.of(
                        group("$category.name", sum("count", 1))
                ))
                .forEach(doc -> result.put(doc.getString("_id"), doc.getInteger("count")));
        return result;
    }

    public Map<String, Integer> countAdsPerCategory() {
        Map<String, Integer> categoryCounts = new HashMap<>();
        for (Document doc : advertisingCollection.find()) {
            String category = doc.get("category", Document.class).getString("name");
            categoryCounts.put(category, categoryCounts.getOrDefault(category, 0) + 1);
        }
        return categoryCounts;
    }


    // Запит 2: Середня вартість рекламних об'єктів у кожній категорії
    public Map<String, Double> aggregateAverageAdPricePerCategory() {
        Map<String, Double> result = new HashMap<>();
        advertisingCollection.aggregate(
                List.of(
                        group("$category.name", avg("averagePrice", "$unitPrice"))
                )
        ).forEach(doc -> result.put(doc.getString("_id"), doc.get("averagePrice", Decimal128.class).doubleValue()));
        return result;
    }

    public Map<String, Double> averageAdPricePerCategory() {
        Map<String, List<Double>> categoryPrices = new HashMap<>();
        for (Document doc : advertisingCollection.find()) {
            String category = doc.get("category", Document.class).getString("name");
            Double price = doc.get("unitPrice", Decimal128.class).doubleValue();
            categoryPrices.computeIfAbsent(category, k -> new ArrayList<>()).add(price);
        }
        return categoryPrices.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0)));
    }


    // Запит 4: Отримання рекламних об'єктів, вартість яких знаходиться в заданому діапазоні (оператор match)
    public Map<String, BigDecimal> aggregateGetAdsInPriceRange(int minPrice, int maxPrice) {
        Map<String, BigDecimal> result = new HashMap<>();
        advertisingCollection.aggregate(
                List.of(
                        match(and(gte("unitPrice", minPrice), lte("unitPrice", maxPrice)))
                )
        ).forEach(doc -> result.put(doc.getString("name"), doc.get("unitPrice", Decimal128.class).bigDecimalValue()));
        return result;
    }

    public Map<String, BigDecimal> getAdsInPriceRange(int minPrice, int maxPrice) {
        Map<String, BigDecimal> result = new HashMap<>();
        for (Document doc : advertisingCollection.find()) {
            BigDecimal price = doc.get("unitPrice", Decimal128.class).bigDecimalValue();
            if (price.compareTo(BigDecimal.valueOf(minPrice)) >= 0 && price.compareTo(BigDecimal.valueOf(maxPrice)) <= 0) {
                result.put(doc.getString("name"), price);
            }
        }
        return result;
    }

/*    public static void main(String[] args) {
        MongoAdvertisingDAO mongoAdvertisingDAO = new MongoAdvertisingDAO();
//        System.out.println(mongoAdvertisingDAO.countAdsPerCategory());
        mongoAdvertisingDAO.aggregateCountAdsPerCategory().entrySet().forEach(System.out::println);
        System.out.println();
//        System.out.println(mongoAdvertisingDAO.averageAdPricePerCategory());
        mongoAdvertisingDAO.aggregateAverageAdPricePerCategory().entrySet().forEach(System.out::println);
        System.out.println();

//        System.out.println(mongoAdvertisingDAO.calculateCampaignCost());
        mongoAdvertisingDAO.aggregateCalculateCampaignCost().entrySet().forEach(System.out::println);
        System.out.println();

//        System.out.println(mongoAdvertisingDAO.getAdsInPriceRange(5, 15));
        mongoAdvertisingDAO.aggregateGetAdsInPriceRange(5, 15).entrySet().forEach(System.out::println);
        System.out.println();

//        System.out.println(mongoAdvertisingDAO.getMostPopularAdCategories(2));
        mongoAdvertisingDAO.aggregateGetMostPopularAdCategories(2).entrySet().forEach(System.out::println);


    }*/


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
