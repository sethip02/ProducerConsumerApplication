package com.application.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/***
 * Chunk class represents a list of records to be uploaded
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Chunk {
    private List<Record> data = new ArrayList<>();
}
