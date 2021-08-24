package com.application.domain;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Chunk {
    List<Record> data = new ArrayList<>();
}
