package org.example.dto;

import lombok.Value;

@Value
public class Identifier {
    IdentityType type;
    String value;
}
