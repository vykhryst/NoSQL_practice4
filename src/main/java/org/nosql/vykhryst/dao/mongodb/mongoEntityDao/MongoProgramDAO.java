package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;
import org.nosql.vykhryst.dao.mongodb.MongoConnectionManager;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Category;
import org.nosql.vykhryst.entity.Client;
import org.nosql.vykhryst.entity.Program;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

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
