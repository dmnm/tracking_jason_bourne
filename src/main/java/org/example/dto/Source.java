package org.example.dto;

import lombok.Value;

@Value
public class Source {
    String id;
    String source;
    Type type;
    String file;

    public enum Type {
        csv, json
    }
}
