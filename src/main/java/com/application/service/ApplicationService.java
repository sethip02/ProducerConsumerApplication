package com.application.service;

import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.exception.UploadException;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
public class ApplicationService {
    static ReentrantLock reentrantLock = null;
    private UUID activeBatchUUID = null;
    RecordComparator recordComparator = new RecordComparator();
    static Map<String, Double> originalPriceList = new ConcurrentHashMap<>();
    static Map<String, Double> cachedPriceList = new HashMap<>();

    public ApplicationService(){
        reentrantLock = new ReentrantLock(true);

    }

    public  UUID startBatchRun(String producerName, int waitTimeForLock) throws InterruptedException {
        log.info(producerName + " is trying to acquire lock.");
        if(reentrantLock.tryLock(waitTimeForLock, TimeUnit.SECONDS)){
            activeBatchUUID = UUID.randomUUID();
            return activeBatchUUID;
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
            cachedPriceList.put(latestRecord.getId(), latestRecord.getPayload().get("Price").asDouble());

        });
        log.info("Chunk uploaded successfully");
        reentrantLock.unlock();



    }

    public  String completeOrCancelBatchRun(UUID batchId, String producerThreadName, String command){

        if(activeBatchUUID != batchId)
            throw new UploadException("Batch run with the id "+ batchId + "is not active. Please check the UUID provided");

        if("complete".equalsIgnoreCase(command))
        {
            //copy the updates from cached list to original list
            cachedPriceList.forEach((k, v) -> {
                originalPriceList.put(k, v);
            });
            cachedPriceList.clear();
        }
        else if("cancel".equalsIgnoreCase(command)){
            cachedPriceList = null;
        }

        log.info("Latest price list: ");
        originalPriceList.forEach((k, v) -> {
            log.info("Value of key "+ k + " is "+ v);
        });
        activeBatchUUID = null;
        reentrantLock.unlock();
        return null;
    }

    public Double getLatestPriceOfInstrument(String consumerThreadName, String id){
        return originalPriceList.get(id);
    }
}
