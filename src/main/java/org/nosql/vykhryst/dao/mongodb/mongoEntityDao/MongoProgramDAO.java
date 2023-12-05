package org.nosql.vykhryst.dao.mongodb.mongoEntityDao;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;
import org.nosql.vykhryst.dao.mongodb.MongoConnectionManager;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Category;
import org.nosql.vykhryst.entity.Client;
import org.nosql.vykhryst.entity.Program;

import java.util.*;

import static java.time.ZoneOffset.UTC;

public class MongoProgramDAO implements ProgramDAO {
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
    public String save(Program entity) {
        Document doc = mapProgramToDocument(entity);
        programCollection.insertOne(doc);
        entity.setId(doc.getObjectId("_id").toString());
        return entity.getId();
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
                .campaignTitle(programDoc.getString("campaignTitle"))
                .description(programDoc.getString("description"))
                .createdAt(programDoc.getDate("createdAt").toInstant().atZone(UTC).toLocalDateTime())
                .client(mapDocumentToClient(programDoc.get("client", Document.class)));

        List<Document> advertisingList = programDoc.getList("advertisingList", Document.class);
        for (Document advertisingListItem : advertisingList) {
            AbstractMap.SimpleEntry<Advertising, Integer> entry = mapAdvertisingListItem(advertisingListItem);
            programBuilder.addAdvertising(entry.getKey(), entry.getValue());
        }
        return programBuilder.build();
    }


    private static AbstractMap.SimpleEntry<Advertising, Integer> mapAdvertisingListItem(Document advertisingDoc) {
        Document adDoc = advertisingDoc.get("advertising", Document.class);
        Advertising advertising = mapDocumentToAdvertising(adDoc);
        return new AbstractMap.SimpleEntry<>(advertising, advertisingDoc.getInteger("quantity"));
    }

    private static Category mapDocumentToCategory(Document categoryDoc) {
        return new Category(categoryDoc.getObjectId("_id").toString(), categoryDoc.getString("name"));
    }

    private static Advertising mapDocumentToAdvertising(Document result) {
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

    private static Client mapDocumentToClient(Document clientDoc) {
        return new Client.Builder()
                .id(clientDoc.getObjectId("_id").toString())
                .username(clientDoc.getString("username"))
                .password(clientDoc.getString("password"))
                .email(clientDoc.getString("email"))
                .firstname(clientDoc.getString("firstname"))
                .lastname(clientDoc.getString("lastname"))
                .phoneNumber(clientDoc.getString("phoneNumber"))
                .build();
    }

    private Document mapProgramToDocument(Program program) {
        Document programDoc = new Document("campaignTitle", program.getCampaignTitle())
                .append("description", program.getDescription())
                .append("createdAt", Date.from(program.getCreatedAt().toInstant(UTC)))
                .append("client", mapClientToDocument(program.getClient()));

        List<Document> advertisingListDocs = new ArrayList<>();
        for (Map.Entry<Advertising, Integer> entry : program.getAdvertisings().entrySet()) {
            Document advertisingListItem = new Document("advertising", mapAdvertisingToDocument(entry.getKey()))
                    .append("quantity", entry.getValue());
            advertisingListDocs.add(advertisingListItem);
        }
        programDoc.append("advertisingList", advertisingListDocs);
        return programDoc;
    }

    private static Document mapAdvertisingToDocument(Advertising advertising) {
        return new Document("_id", new ObjectId(advertising.getId()))
                .append("name", advertising.getName())
                .append("description", advertising.getDescription())
                .append("measurement", advertising.getMeasurement())
                .append("unitPrice", new Decimal128(advertising.getUnitPrice()))
                .append("updatedAt", advertising.getUpdatedAt())
                .append("category", new Document("_id", new ObjectId(advertising.getCategory().getId()))
                        .append("name", advertising.getCategory().getName()));
    }

    private static Document mapClientToDocument(Client client) {
        return new Document("_id", new ObjectId(client.getId()))
                .append("username", client.getUsername())
                .append("firstname", client.getFirstname())
                .append("lastname", client.getLastname())
                .append("phoneNumber", client.getPhoneNumber())
                .append("email", client.getEmail())
                .append("password", client.getPassword());
    }

    private Document createIdQuery(String id) {
        return new Document("_id", new ObjectId(id));
    }
}
