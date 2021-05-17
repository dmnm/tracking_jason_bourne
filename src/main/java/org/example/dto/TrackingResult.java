package org.example.dto;

import lombok.Value;

@Value
public class TrackingResult {
    String file;
    String dateTime;
    String name;

    @Override
    public String toString() {
        return file + ": " + dateTime + ", " + name;
    }
}
