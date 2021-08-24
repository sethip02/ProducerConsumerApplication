package com.application.domain;

import java.util.Comparator;

public class RecordComparator implements Comparator {
    @Override
    public int compare(Object r1, Object r2) {
        return ((Record)r2).getAsOf().compareTo(((Record)r1).getAsOf());
    }
}
