package com.application.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class Chunk {
    List<Record> data = new ArrayList<>();
}
