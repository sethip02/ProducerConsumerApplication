package com.application.domain;

import java.util.Comparator;

/***
 * RecordComparator class is used to compare the records based on the asOf attribute
 */
public class RecordComparator implements Comparator {
    @Override
    public int compare(Object r1, Object r2) {
        return ((Record)r2).getAsOf().compareTo(((Record)r1).getAsOf());
    }
}
