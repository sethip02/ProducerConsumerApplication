package com.application.domain;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

/***
 * Record class represents an entry for a instrument price update
 *
 */
@Data
@AllArgsConstructor
public class Record {
    //id represents instrument id
    String id;
    //asOf field represents the datetime when the price was updated
    LocalDateTime asOf;
    //payload represents the metadata about the update. price is one the metadata.
    JsonNode payload;

    @Override
    public String toString() {
        return "Record{" +
                "id='" + id + '\'' +
                ", asOf=" + asOf.toString() +
                ", payload=" + payload.toString() +
                '}';
    }
}
