package com.application.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/***
 * Record class represents an entry for a instrument price update
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Record {
    //id represents instrument id
    private String id;
    //asOf field represents the datetime when the price was updated
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime asOf;
    //payload represents the metadata about the update. price is one the metadata.
    private JsonNode payload;

    @Override
    public String toString() {
        return "Record{" +
                "id='" + id + '\'' +
                ", asOf=" + asOf.toString() +
                ", payload=" + payload.toString() +
                '}';
    }
}
