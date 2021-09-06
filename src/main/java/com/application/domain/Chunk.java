package com.application.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/***
 * Chunk class represents a list of records to be uploaded
 */
@Data
@AllArgsConstructor
public class Chunk {
    private List<Record> data = new ArrayList<>();
}
