package com.application.service;

import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.exception.UploadException;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ApplicationService {
    static ReentrantLock reentrantLock = null;
    static UUID activeBatchUUID = null;
    RecordComparator recordComparator = new RecordComparator();
    Map<String, Long> latestPriceList = new ConcurrentHashMap<>();

    public ApplicationService(){
        this.reentrantLock = new ReentrantLock();

    }

    public  UUID startBatchRun(){
        if(reentrantLock.tryLock()){
            return UUID.randomUUID();
        }
        else{
            return null;

        }


    }


    public  void uploadChunkOfRecords(UUID batchId, Chunk chunk){
        reentrantLock.lock();
        if(activeBatchUUID != batchId)
            throw new UploadException("Batch run with the id "+ batchId + "is not active. Please check the UUID provided");


        List<Record> recordsToBeProcessed = chunk.getData();
        Map<String, List<Record>> groupedRecords = recordsToBeProcessed.stream().collect(Collectors.groupingBy(Record::getId));
        groupedRecords.forEach((s, l) -> {
            l.sort(recordComparator);
            Record latestRecord = l.get(0);
            latestPriceList.put(latestRecord.getId(), latestRecord.getPayload().get("Price").asLong());

        });

        reentrantLock.unlock();



    }

    public  String completeOrCancelBatchRun(UUID batchId, String producerThreadName, String command){
        activeBatchUUID = null;
        reentrantLock.unlock();
        return null;
    }

    public Long getLatestPriceOfInstrument(String id){
        return latestPriceList.get(id);
    }
}
