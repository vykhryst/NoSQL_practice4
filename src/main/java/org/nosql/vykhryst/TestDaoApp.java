package org.nosql.vykhryst;

import org.nosql.vykhryst.dao.AbstractDaoFactory;
import org.nosql.vykhryst.dao.DaoFactory;
import org.nosql.vykhryst.dao.TypeDAO;
import org.nosql.vykhryst.dao.entityDao.AdvertisingDAO;
import org.nosql.vykhryst.dao.entityDao.CategoryDAO;
import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.entityDao.ProgramDAO;
import org.nosql.vykhryst.entity.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

public class TestDaoApp {

    public static void main(String[] args) {
        // Initialize DAO Factory
        AbstractDaoFactory daoFactory = DaoFactory.getInstance();

        TypeDAO typeDAO = TypeDAO.MYSQL;

//        // Test Category
        testCategory(daoFactory.getCategoryDAO(typeDAO));

//        // Test Advertising
        testAdvertising(daoFactory.getAdvertisingDAO(typeDAO), daoFactory.getCategoryDAO(typeDAO));

//        // Test Client
        testClient(daoFactory.getClientDAO(typeDAO));

        // Test Program
        testProgram(daoFactory.getProgramDAO(typeDAO), daoFactory.getAdvertisingDAO(typeDAO));
    }

    private static void testCategory(CategoryDAO categoryDAO) {
        System.out.println("----- Testing Category -----");

        // Find All Categories
        System.out.println("All Categories:");
        categoryDAO.findAll().forEach(System.out::println);
        System.out.println();

        // Add Category
        Category category = new Category();
        category.setName("TEST Category");
        categoryDAO.save(category);
        System.out.println("Added Category: " + categoryDAO.findById(category.getId()).orElse(null));
        String categoryId = category.getId();

        // Find Category by ID
        Optional<Category> foundCategory = categoryDAO.findById(categoryId);
        System.out.println("Found Category by ID: " + foundCategory.orElse(null));

        // Update Category
        category.setName("Updated Category");
        System.out.println("Updating Category: " + categoryDAO.update(category));
        System.out.println("Updated Category: " + categoryDAO.findById(categoryId).orElse(null));

        // Delete Category
        System.out.println("Deleting Category: " + categoryDAO.delete(category.getId()));
        System.out.println("Found Category by ID: " + categoryDAO.findById(categoryId).orElse(null));
    }

    private static void testAdvertising(AdvertisingDAO advertisingDAO, CategoryDAO categoryDAO) {
        System.out.println("\n----- Testing Advertising -----");

        // Add Advertising
//        Category category = categoryDAO.findById("6570b88efec1bc4c7e89874e").orElse(null);
        Category category = categoryDAO.findById(String.valueOf(3)).orElse(null);
        Advertising advertising = new Advertising.Builder()
                .category(category)
                .name("TEST New TV Ad")
                .measurement("sec")
                .unitPrice(new BigDecimal("100.00"))
                .description("TEST Description")
                .updatedAt(LocalDateTime.now())
                .build();
        advertisingDAO.save(advertising);
        System.out.println("Added Advertising: " + advertising);

        // Find Advertising by ID
        Optional<Advertising> foundAdvertising = advertisingDAO.findById(advertising.getId());
        System.out.println("Found Advertising by ID: " + foundAdvertising.orElse(null));

        // Find Advertising by Name
        Optional<Advertising> foundByName = advertisingDAO.findByName("TEST New TV Ad");
        System.out.println("Found Advertising by Name: " + foundByName.orElse(null));

        // Update Advertising
        advertising.setDescription("Updated description");
        advertisingDAO.update(advertising);
        System.out.println("Updated Advertising: " + advertisingDAO.findById(advertising.getId()).orElse(null));

        // Delete Advertising
        advertisingDAO.delete(advertising.getId());
        System.out.println("Deleting Advertising");
        System.out.println("Found Advertising by ID: " + advertisingDAO.findById(advertising.getId()).orElse(null));
    }

    private static void testClient(ClientDAO clientDAO) {
        System.out.println("\n----- Testing Client -----");

        // Add Client
        Client client = new Client.Builder()
                .username("test_user")
                .firstname("John")
                .lastname("Snow")
                .phoneNumber("1212343490")
                .email("john.snow@example.com")
                .password("test_password")
                .build();
        System.out.println("Added Client ID: " + clientDAO.save(client));

        // Find Client by ID
        Optional<Client> foundClient = clientDAO.findById(client.getId());
        System.out.println("Found Client by ID: " + foundClient.orElse(null));

        // Delete Client and Programs for MySQL
        System.out.println("Deleting Client and Programs: " + clientDAO.deleteClientAndPrograms(6));
        System.out.println("Found Client by ID: " + clientDAO.findById(String.valueOf(6)).orElse(null));

        // Find Client by Username
        Optional<Client> foundByUsername = clientDAO.findByUsername("test_user");
        System.out.println("Found Client by Username: " + foundByUsername.orElse(null));

        // Update Client
        client.setFirstname("Updated John");
        client.setUsername("updated_username");
        System.out.println("Updating Client: " + clientDAO.update(client));
        System.out.println("Updated Client: " + clientDAO.findById(client.getId()).orElse(null));

        // Delete Client
        System.out.println("Deleting Client: " + clientDAO.delete(client.getId()));
        System.out.println("Found Client by ID: " + clientDAO.findById(client.getId()).orElse(null));
    }

    private static void testProgram(ProgramDAO programDAO, AdvertisingDAO advertisingDAO) {
        System.out.println("\n----- Testing Program -----");

        // Find All Programs
//        System.out.println("All Programs:");
//        programDAO.findAll().forEach(System.out::println);

        // Find Advertising by ID
//        Advertising advertising1 = advertisingDAO.findById("6570b88ffec1bc4c7e898757").orElse(null);
        Advertising advertising1 = advertisingDAO.findById(String.valueOf(20)).orElse(null);
        System.out.println("Found Advertising 1 by ID: " + advertising1);

//        Advertising advertising2 = advertisingDAO.findById("6570b88ffec1bc4c7e898758").orElse(null);
        Advertising advertising2 = advertisingDAO.findById(String.valueOf(10)).orElse(null);
        System.out.println("Found Advertising 2 by ID: " + advertising2);

        // Add Program
//        Program program = new Program.Builder().client(new Client.Builder().id("6570b88ffec1bc4c7e89877b").build())
        Program program = new Program.Builder().client(new Client.Builder().id(String.valueOf(10)).build())
                .campaignTitle("TEST Campaign")
                .description("TEST Description")
                .createdAt(LocalDateTime.now())
                .addAdvertising(advertising1, 3)
                .addAdvertising(advertising2, 5)
                .build();

        System.out.println("\nAdded Program: " + programDAO.save(program));

        // Find Program by ID
        Optional<Program> foundProgram = programDAO.findById(program.getId());
        System.out.println("\nFound added Program by ID: " + foundProgram.orElse(null));


        // Add Advertising to Program for MySQL
        Advertising advertising3 = advertisingDAO.findById(String.valueOf(13)).orElse(null);
        programDAO.saveAdvertisingToProgram(program.getId(), Map.of(advertising3, 10));
        System.out.println("\nAdded Advertising to Program: " + advertising3);


        // Update Program Advertising
        // for MongoDB
//        Advertising advertising3 = advertisingDAO.findById("6570b88ffec1bc4c7e89875f").orElse(null);
        program.addAdvertising(advertising3, 30);
        System.out.println("\nUpdating Program Advertising: " + programDAO.update(program));
        System.out.println("Result: " + programDAO.findById(program.getId()).orElse(null));

        // Delete Advertising from Program
        program.deleteAdvertising(advertising3);
        System.out.println("\nDeleting Advertising from Program: " + programDAO.update(program));
        System.out.println("\nDeleting Advertising from Program: " + programDAO.deleteAdvertisingFromProgram(program.getId(), advertising3.getId()));
        System.out.println("Result: " + programDAO.findById(program.getId()).orElse(null));

        // Delete Program
        System.out.println("\nDeleting Program: " + programDAO.delete(program.getId()));
        System.out.println("Found Program by ID: " + programDAO.findById(program.getId()).orElse(null));
    }
}
