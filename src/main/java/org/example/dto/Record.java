package org.example.dto;

import lombok.Value;

import java.util.Collection;

@Value
public class Record {
    Source source;
    String dateTime; // as of now not sure if need to parse it somehow
    String fullName;
    Collection<Identifier> identifiers;
}
