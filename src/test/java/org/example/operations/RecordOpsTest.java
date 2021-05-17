package org.example.operations;

import lombok.val;
import org.example.dto.Identifier;
import org.example.dto.Source;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;

import static org.example.dto.IdentityType.name;
import static org.example.dto.IdentityType.passport;
import static org.example.dto.IdentityType.visa;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordOpsTest {

    RecordOps underTest = new RecordOps();

    @Test
    public void testCsvRecords() {
        val dateTime = "2021-03-08T00:00:00.0000Z";
        val lines = Arrays.asList(
            "TimeStamp , Name, Passport, Visa",
            dateTime + ",Hello,Visa,World",
            dateTime + " ,Jason,Bourne,Again"
        );
        val source = new Source("123", "any_source", Source.Type.csv, "doesn't/matter.csv");
        val records = underTest.csvRecords(source, Flux.fromIterable(lines))
                               .collectList()
                               .block();

        assertNotNull(records);
        assertEquals(2, records.size());

        {
            val record = records.get(0);
            assertEquals(source, record.getSource());
            assertEquals(dateTime, record.getDateTime());

            assertTrue(record.getIdentifiers().contains(new Identifier(name, "Hello")));
            assertTrue(record.getIdentifiers().contains(new Identifier(passport, "Visa")));
            assertTrue(record.getIdentifiers().contains(new Identifier(visa, "World")));
        }
        {
            val record = records.get(1);
            assertEquals(source, record.getSource());
            assertEquals(dateTime, record.getDateTime());

            assertTrue(record.getIdentifiers().contains(new Identifier(name, "Jason")));
            assertTrue(record.getIdentifiers().contains(new Identifier(passport, "Bourne")));
            assertTrue(record.getIdentifiers().contains(new Identifier(visa, "Again")));
        }
    }

    @Test
    public void testZipWithHeaders() {
        val headers = List.of(name, passport, visa);
        val lines = Arrays.asList(
            "Timestamp, Name, Passport, Visa",
            "2021-03-08T00:00:00.0000Z,Hello,Visa,World",
            "2021-03-08T00:00:00.0000Z, Jason,Bourne,Again"
        );

        val result = underTest.zipWithHeaders(Flux.fromIterable(lines))
                              .collectList()
                              .block();

        assertNotNull(result);
        assertEquals(2, result.size());

        assertEquals(headers, result.get(0).getT1());
        assertEquals(lines.get(1), result.get(0).getT2());

        assertEquals(headers, result.get(1).getT1());
        assertEquals(lines.get(2), result.get(1).getT2());
    }
}