package com.application.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class Record {
    String id;
    LocalDateTime asOf;
    JsonNode payload;

    @Override
    public String toString() {
        return "Record{" +
                "id='" + id + '\'' +
                ", asOf=" + asOf.toString() +
                ", payload=" + payload.asText() +
                '}';
    }
}
