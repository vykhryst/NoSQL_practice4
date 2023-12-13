package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;
import org.nosql.vykhryst.dao.mongodb.MongoConnectionManager;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Category;
import org.nosql.vykhryst.entity.Client;
import org.nosql.vykhryst.entity.Program;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Sorts.descending;
import static java.time.ZoneOffset.UTC;

public class MongoProgramDAO implements ProgramDAO {
    public static final String CAMPAIGN_TITLE = "campaignTitle";
    public static final String DESCRIPTION = "description";
    public static final String CREATED_AT = "createdAt";
    public static final String CLIENT = "client";
    public static final String QUANTITY = "quantity";
    public static final String ADVERTISING = "advertising";
    public static final String ADVERTISING_LIST = "advertisingList";
    public static final String CATEGORY = "category";
    public static final String MEASUREMENT = "measurement";
    public static final String UNIT_PRICE = "unitPrice";
    public static final String NAME = "name";
    public static final String EMAIL = "email";
    public static final String PASSWORD = "password";
    public static final String UPDATED_AT = "updatedAt";
    private final MongoCollection<Document> programCollection;

    public MongoProgramDAO() {
        this.programCollection = MongoConnectionManager.getCollection("program");
    }

    @Override
    public Optional<Program> findById(String id) {
        Document query = createIdQuery(id);
        Document program = programCollection.find(query).first();
        return program != null ? Optional.of(mapDocumentToProgram(program)) : Optional.empty();
    }

    @Override
    public List<Program> findAll() {
        List<Program> programs = new ArrayList<>();
        for (Document doc : programCollection.find()) {
            programs.add(mapDocumentToProgram(doc));
        }
        return programs;
    }

    @Override
    public String save(Program program) {
        Document doc = mapProgramToDocument(program);
        programCollection.insertOne(doc);
        program.setId(doc.getObjectId("_id").toString());
        return program.getId();
    }


    // Запит 3: Вартість кожної рекламної кампанії
    public Map<String, BigDecimal> aggregateCalculateProgramCost() {
        Map<String, BigDecimal> result = new HashMap<>();
        List<Bson> pipeline = Arrays.asList(
                Aggregates.unwind("$advertisingList"),
                Aggregates.project(
                        Projections.fields(
                                Projections.include("campaignTitle"),
                                Projections.computed("cost",
                                        new Document("$multiply",
                                                Arrays.asList("$advertisingList.advertising.unitPrice", "$advertisingList.quantity")
                                        )
                                )
                        )
                ),
                Aggregates.group("$campaignTitle", Accumulators.sum("totalCost", "$cost"))
        );

        programCollection.aggregate(pipeline).forEach(doc ->
                result.put(doc.getString("_id"), doc.get("totalCost", Decimal128.class).bigDecimalValue())
        );
        return result;
    }

    public Map<String, BigDecimal> calculateProgramCost() {
        Map<String, BigDecimal> campaignCosts = new HashMap<>();
        for (Document doc : programCollection.find()) {
            String campaignTitle = doc.getString("campaignTitle");
            List<Document> advertisingList = (List<Document>) doc.get("advertisingList");
            for (Document ad : advertisingList) {
                BigDecimal unitPrice = ad.get("advertising", Document.class).get("unitPrice", Decimal128.class).bigDecimalValue();
                Integer quantity = ad.getInteger("quantity");
                campaignCosts.put(campaignTitle, campaignCosts.getOrDefault(campaignTitle, BigDecimal.valueOf(0.0))
                        .add(unitPrice.multiply(BigDecimal.valueOf(quantity))));
            }
        }
        return campaignCosts;
    }

    // Запит 5: Отримання найбільш популярну категорію реклами за кількістю рекламних програм
    public Map<String, Integer> aggregateGetMostPopularAdCategories(int limit) {
        Map<String, Integer> result = new HashMap<>();
        programCollection.aggregate(
                        List.of(
                                unwind("$advertisingList"),
                                group("$advertisingList.advertising.category.name", sum("count", 1)),
                                sort(descending("count")),
                                limit(limit)
                        ))
                .forEach(doc -> result.put(doc.getString("_id"), doc.getInteger("count")));
        return result;
    }

    public Map<String, Integer> getMostPopularAdCategories(int limit) {
        Map<String, Integer> categoryCounts = new HashMap<>();
        for (Document doc : programCollection.find()) {
            List<Document> advertisingList = (List<Document>) doc.get("advertisingList");
            for (Document ad : advertisingList) {
                String category = ad.get("advertising", Document.class).get("category", Document.class).getString("name");
                categoryCounts.put(category, categoryCounts.getOrDefault(category, 0) + 1);
            }
        }
        return categoryCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(limit)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    @Override
    public boolean saveAdvertisingToProgram(String programId, Map<Advertising, Integer> advertising) {
        return false;
    }

    @Override
    public boolean deleteAdvertisingFromProgram(String programId, String advertisingId) {
        return false;
    }

    @Override
    public String migrate(Program program) {
        Document programDoc = mapProgramToDocumentMigration(program);
        programCollection.insertOne(programDoc);
        program.setId(programDoc.getObjectId("_id").toString());
        return program.getId();
    }


    @Override
    public boolean update(Program entity) {
        Document query = createIdQuery(entity.getId());
        Document doc = mapProgramToDocument(entity);
        return programCollection.updateOne(query, new Document("$set", doc)).wasAcknowledged();
    }

    @Override
    public boolean delete(String id) {
        Document query = createIdQuery(id);
        return programCollection.deleteOne(query).wasAcknowledged();
    }


    // Utility methods
    private Program mapDocumentToProgram(Document programDoc) {
        Program.Builder programBuilder = new Program.Builder()
                .id(programDoc.getObjectId("_id").toString())
                .campaignTitle(programDoc.getString(CAMPAIGN_TITLE))
                .description(programDoc.getString(DESCRIPTION))
                .createdAt(programDoc.getDate(CREATED_AT).toInstant().atZone(UTC).toLocalDateTime())
                .client(mapDocumentToClient(programDoc.get(CLIENT, Document.class)));

        List<Document> advertisingList = programDoc.getList(ADVERTISING_LIST, Document.class);
        for (Document advertisingListItem : advertisingList) {
            AbstractMap.SimpleEntry<Advertising, Integer> entry = mapAdvertisingListItem(advertisingListItem);
            programBuilder.addAdvertising(entry.getKey(), entry.getValue());
        }
        return programBuilder.build();
    }


    private static AbstractMap.SimpleEntry<Advertising, Integer> mapAdvertisingListItem(Document advertisingDoc) {
        Document adDoc = advertisingDoc.get(ADVERTISING, Document.class);
        Advertising advertising = mapDocumentToAdvertising(adDoc);
        return new AbstractMap.SimpleEntry<>(advertising, advertisingDoc.getInteger(QUANTITY));
    }

    private static Category mapDocumentToCategory(Document categoryDoc) {
        return new Category(categoryDoc.getObjectId("_id").toString(), categoryDoc.getString(NAME));
    }

    private static Advertising mapDocumentToAdvertising(Document result) {
        return new Advertising.Builder()
                .id(result.getObjectId("_id").toString())
                .name(result.getString(NAME))
                .description(result.getString(DESCRIPTION))
                .measurement(result.getString(MEASUREMENT))
                .unitPrice(result.get(UNIT_PRICE, Decimal128.class).bigDecimalValue())
                .updatedAt(result.getDate(UPDATED_AT).toInstant().atZone(UTC).toLocalDateTime())
                .category(mapDocumentToCategory(result.get(CATEGORY, Document.class)))
                .build();
    }

    private static Client mapDocumentToClient(Document clientDoc) {
        return new Client.Builder()
                .id(clientDoc.getObjectId("_id").toString())
                .username(clientDoc.getString("username"))
                .password(clientDoc.getString(PASSWORD))
                .email(clientDoc.getString(EMAIL))
                .firstname(clientDoc.getString("firstname"))
                .lastname(clientDoc.getString("lastname"))
                .phoneNumber(clientDoc.getString("phoneNumber"))
                .build();
    }

    private Document mapProgramToDocument(Program program) {
        Document programDoc = new Document(CAMPAIGN_TITLE, program.getCampaignTitle())
                .append(DESCRIPTION, program.getDescription())
                .append(CREATED_AT, Date.from(program.getCreatedAt().toInstant(UTC)))
                .append(CLIENT, mapClientToDocument(program.getClient()));

        List<Document> advertisingListDocs = new ArrayList<>();
        for (Map.Entry<Advertising, Integer> entry : program.getAdvertisings().entrySet()) {
            Document advertisingListItem = new Document(ADVERTISING, mapAdvertisingToDocument(entry.getKey()))
                    .append(QUANTITY, entry.getValue());
            advertisingListDocs.add(advertisingListItem);
        }
        programDoc.append(ADVERTISING_LIST, advertisingListDocs);
        return programDoc;
    }

    private static Document mapAdvertisingToDocument(Advertising advertising) {
        return new Document("_id", new ObjectId(advertising.getId()))
                .append(NAME, advertising.getName())
                .append(DESCRIPTION, advertising.getDescription())
                .append(MEASUREMENT, advertising.getMeasurement())
                .append(UNIT_PRICE, new Decimal128(advertising.getUnitPrice()))
                .append(UPDATED_AT, advertising.getUpdatedAt())
                .append(CATEGORY, new Document("_id", new ObjectId(advertising.getCategory().getId()))
                        .append(NAME, advertising.getCategory().getName()));
    }

    private static Document mapClientToDocument(Client client) {
        return new Document("_id", new ObjectId(client.getId()))
                .append("username", client.getUsername())
                .append("firstname", client.getFirstname())
                .append("lastname", client.getLastname())
                .append("phoneNumber", client.getPhoneNumber())
                .append(EMAIL, client.getEmail())
                .append(PASSWORD, client.getPassword());
    }

    private Document mapProgramToDocumentMigration(Program program) {
        Document programDoc = new Document(CAMPAIGN_TITLE, program.getCampaignTitle())
                .append(DESCRIPTION, program.getDescription())
                .append(CREATED_AT, Date.from(program.getCreatedAt().toInstant(UTC)))
                .append(CLIENT, mapClientToDocument(Objects.requireNonNull(findClientByEmailAndPassword(
                        program.getClient().getEmail(), program.getClient().getPassword()))));

        List<Document> advertisingListDocs = new ArrayList<>();
        for (Map.Entry<Advertising, Integer> entry : program.getAdvertisings().entrySet()) {
            Document advertisingListItem = new Document(ADVERTISING, mapAdvertisingToDocumentMigration(
                    Objects.requireNonNull(findAdvertisingByMultipleKeys(entry.getKey().getName(), entry.getKey().getMeasurement(), entry.getKey().getUnitPrice()))))
                    .append(QUANTITY, entry.getValue());
            advertisingListDocs.add(advertisingListItem);
        }
        programDoc.append(ADVERTISING_LIST, advertisingListDocs);
        return programDoc;
    }

    private Document mapAdvertisingToDocumentMigration(Advertising advertising) {
        Category category = findCategoryByName(advertising.getCategory().getName());
        advertising.setCategory(category);
        return mapAdvertisingToDocument(advertising);
    }

    private Category findCategoryByName(String name) {
        Document query = new Document(NAME, name);
        Document categoryDoc = MongoConnectionManager.getCollection(CATEGORY).find(query).first();
        return categoryDoc != null ? mapDocumentToCategory(categoryDoc) : null;
    }

    private Advertising findAdvertisingByMultipleKeys(String name, String measurement, BigDecimal unitPrice) {
        Document query = new Document(NAME, name).append(MEASUREMENT, measurement).append(UNIT_PRICE, new Decimal128(unitPrice));
        Document adDoc = MongoConnectionManager.getCollection(ADVERTISING).find(query).first();
        return adDoc != null ? mapDocumentToAdvertising(adDoc) : null;
    }

    private Client findClientByEmailAndPassword(String email, String password) {
        Document query = new Document(EMAIL, email).append(PASSWORD, password);
        Document clientDoc = MongoConnectionManager.getCollection(CLIENT).find(query).first();
        return clientDoc != null ? mapDocumentToClient(clientDoc) : null;
    }

    private Document createIdQuery(String id) {
        return new Document("_id", new ObjectId(id));
    }
}
