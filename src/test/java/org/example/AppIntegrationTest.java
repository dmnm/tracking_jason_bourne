package org.example;

import lombok.val;
import org.example.dto.IdentityType;
import org.example.dto.TrackingResult;
import org.example.service.LuceneIndex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AppIntegrationTest {
    @Test
    public void testDataIngestion() {
        try (val application = new Application()) {
            application.runDataIngestion(new String[]{"src/test/resources/test_1.csv", "src/test/resources/test_2.csv"});

            val index = LuceneIndex.instance();
            val records = index.fuzzySearch(IdentityType.national_id.name(), "012-3456-7890");
            assertEquals(4, records.size());
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void testFullWorkFlow() {
        var results = ConcurrentHashMap.newKeySet();
        try (val application = new Application()) {
            application.runDataIngestion(new String[]{"src/test/resources/test_3.csv", "src/test/resources/test_4.csv"});
            application.runTracking(
                "Jason Bourne",
                flux -> flux.doOnNext(results::add).blockLast()
            );

            assertEquals(4, results.size());
            assertTrue(results.contains(new TrackingResult("src/test/resources/test_3.csv", "2021-05-05T00:00:00.0000Z", "Jason Bourne")));
            assertTrue(results.contains(new TrackingResult("src/test/resources/test_3.csv", "2021-05-05T02:00:00.0000Z", "George  P. Washburn")));
            assertTrue(results.contains(new TrackingResult("src/test/resources/test_3.csv", "2021-05-05T01:00:00.0000Z", "David Webb")));
            assertTrue(results.contains(new TrackingResult("src/test/resources/test_4.csv", "2021-05-05T11:00:02.0000Z", "David Web")));
        } catch (Exception e) {
            fail(e);
        }
    }
}
