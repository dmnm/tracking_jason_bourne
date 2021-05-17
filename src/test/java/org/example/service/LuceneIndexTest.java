package org.example.service;

import lombok.val;
import org.example.dto.Identifier;
import org.example.dto.Record;
import org.example.dto.Source;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.example.dto.IdentityType.bank_card;
import static org.example.dto.IdentityType.flight_ticket;
import static org.example.dto.IdentityType.national_id;
import static org.example.dto.Source.Type.json;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LuceneIndexTest {

    LuceneIndex underTest = LuceneIndex.instance();

    @Test
    public void testSimple() {
        underTest.index("name", "Jason~Bourne");
        underTest.index("name", "Jakson&Bourn");
        underTest.commit();

        val records = underTest.fuzzySearch("name", "Jason_Bourne");
        assertEquals(2, records.size());
    }

    @Test
    public void testIndexRecord() {
        val source = new Source("42", "source_n1", json, "whatever/path.json");
        val dateTime = "2020-05-10T00:00:00.0000Z";
        val record = new Record(
            source,
            dateTime,
            "John Doe",
            Arrays.asList(
                new Identifier(national_id, "098-765-432-1"),
                new Identifier(bank_card, "1234-5678-9012-3456"),
                new Identifier(flight_ticket, "J6 42C")
            )
        );

        underTest.index(record);
        underTest.commit();

        val records = underTest.fuzzySearch("bank_card", "_234-5678-9012-345_");
        assertEquals(1, records.size());

        val result = records.iterator().next();
        assertEquals(source, result.getSource());
        assertEquals(dateTime, result.getDateTime());

        assertTrue(result.getIdentifiers().contains(new Identifier(flight_ticket, "J6 42C")));
        assertTrue(result.getIdentifiers().contains(new Identifier(bank_card, "1234-5678-9012-3456")));
        assertTrue(result.getIdentifiers().contains(new Identifier(national_id, "098-765-432-1")));
    }
}