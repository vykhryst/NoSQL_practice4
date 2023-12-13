package org.nosql.vykhryst.testApps;

import org.nosql.vykhryst.dao.entityDao.ClientDAO;
import org.nosql.vykhryst.dao.mongodb.mongoEntityDao.MongoAdvertisingDAO;
import org.nosql.vykhryst.dao.mongodb.mongoEntityDao.MongoProgramDAO;
import org.nosql.vykhryst.entity.Client;

import java.time.Duration;
import java.time.Instant;

public class TestAggregation {

    public static void main(String[] args) {
        MongoProgramDAO mongoProgramDAO = new MongoProgramDAO();
        MongoAdvertisingDAO mongoAdvertisingDAO = new MongoAdvertisingDAO();

        // Запит 1
        Instant start = Instant.now();
        mongoAdvertisingDAO.countAdsPerCategory();
        Instant end = Instant.now();
        System.out.println("Time for countAdsPerCategory: " + Duration.between(start, end).toMillis() + " ms");

        start = Instant.now();
        mongoAdvertisingDAO.aggregateCountAdsPerCategory();
        end = Instant.now();
        System.out.println("Time for aggregateCountAdsPerCategory: " + Duration.between(start, end).toMillis() + " ms");

        // Запит 2
        start = Instant.now();
        mongoAdvertisingDAO.averageAdPricePerCategory();
        end = Instant.now();
        System.out.println("Time for averageAdPricePerCategory: " + Duration.between(start, end).toMillis() + " ms");

        start = Instant.now();
        mongoAdvertisingDAO.aggregateAverageAdPricePerCategory();
        end = Instant.now();
        System.out.println("Time for aggregateAverageAdPricePerCategory: " + Duration.between(start, end).toMillis() + " ms");


        // Запит 3
        start = Instant.now();
        mongoProgramDAO.calculateProgramCost();
        end = Instant.now();
        System.out.println("Time for calculateProgramCost: " + Duration.between(start, end).toMillis() + " ms");

        start = Instant.now();
        mongoProgramDAO.aggregateCalculateProgramCost();
        end = Instant.now();
        System.out.println("Time for aggregateCalculateProgramCost: " + Duration.between(start, end).toMillis() + " ms");


        // Запит 4
        start = Instant.now();
        mongoAdvertisingDAO.getAdsInPriceRange(100, 200);
        end = Instant.now();
        System.out.println("Time for getAdsInPriceRange: " + Duration.between(start, end).toMillis() + " ms");

        start = Instant.now();
        mongoAdvertisingDAO.aggregateGetAdsInPriceRange(100, 200);
        end = Instant.now();
        System.out.println("Time for aggregateGetAdsInPriceRange: " + Duration.between(start, end).toMillis() + " ms");


        // Запит 5
        start = Instant.now();
        mongoProgramDAO.getMostPopularAdCategories(3);
        end = Instant.now();
        System.out.println("Time for getMostPopularAdCategories: " + Duration.between(start, end).toMillis() + " ms");

        start = Instant.now();
        mongoProgramDAO.aggregateGetMostPopularAdCategories(3);
        end = Instant.now();
        System.out.println("Time for aggregateGetMostPopularAdCategories: " + Duration.between(start, end).toMillis() + " ms");
    }
}
