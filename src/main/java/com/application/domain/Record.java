package com.application.domain;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Record {
    String id;
    LocalDateTime asOf;
    JsonNode payload;
}
