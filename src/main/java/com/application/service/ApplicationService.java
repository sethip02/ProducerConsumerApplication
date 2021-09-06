package com.application.service;

import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.exception.PriceAccessException;
import com.application.exception.UploadException;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/***
 * ApplicationService represents the service which offers batch upload functionality to the producers
 * and maintains the latest price of all the instruments to be made available for consumers.
 */
@Slf4j
public class ApplicationService {
    private static ReentrantLock reentrantLock = new ReentrantLock(true);;
    private static List<UUID> activeBatchIDs = new ArrayList<>();
    private RecordComparator recordComparator = new RecordComparator();
    private static Map<String, Record> originalPriceList = new ConcurrentHashMap<>();
    private Map<String, Record> cachedPriceList = new HashMap<>();


    /***
     * startBatchRun method is used by the producer thread to start a batch
     * A lock is acquired before starting a batch run.
     * @param producerName - producer name which tries to start the batch run
     * @return
     * @throws InterruptedException
     */
    public UUID startBatchRun(String producerName) {
        log.info(producerName + " is starting a batch run.");
        UUID newBatchID = UUID.randomUUID();
        activeBatchIDs.add(newBatchID);
        return newBatchID;
    }

    /***
     * uploadChunkOfRecords method is used to upload a chunk of records
     * Method will maintain a cached list of latest prices of the instruments after applying the record updates.
     * Original instrument price list remains untouched.
     * New update lock is acquired by the producer thread before an upload is done.
     * @param batchId - batch id under which particular chunk is processed
     * @param chunk - list of records.
     */
    public void uploadChunkOfRecords(UUID batchId, Chunk chunk) {
        if (! activeBatchIDs.contains(batchId)) {
            throw new UploadException("Batch run with the id " + batchId + "is not active. Please check the UUID provided");
        }


        List<Record> recordsToBeProcessed = chunk.getData();
        Map<String, List<Record>> groupedRecords = recordsToBeProcessed.stream().collect(Collectors.groupingBy(Record::getId));
        groupedRecords.forEach((s, l) -> {
            l.sort(recordComparator);
            Record latestRecord = l.get(0);
            cachedPriceList.put(latestRecord.getId(), latestRecord);

        });
        log.info("Chunk uploaded successfully");

    }

    /***
     * completeOrCancelBatchRun method is used to complete or cancel a batch run
     * based on the outcome of the all the chunks upload.
     * For complete request, the cached price list is merged with the original price list
     * For cancel request , the cached price list is discarded with no change to the original price list.
     * @param batchId - batch id
     * @param producerThreadName - producer name
     * @param command - "complete" or "cancel"
     * @return
     */
    public void completeOrCancelBatchRun(UUID batchId, String producerThreadName, String command) {

        if (! activeBatchIDs.contains(batchId)) {
            throw new UploadException("Batch run with the id " + batchId + "is not active. Please check the UUID provided");
        }
        //Acquire a lock to update the original price list.
        reentrantLock.lock();

        if ("complete".equalsIgnoreCase(command)) {
            //copy the updates from cached list to original list
            cachedPriceList.forEach((k, v) -> {
                Record originalRecord = originalPriceList.get(k);
                if(originalRecord != null) {
                    if (v.getAsOf().compareTo(originalRecord.getAsOf()) >= 0) {
                        originalPriceList.put(k, v);
                    }
                }else{
                    originalPriceList.put(k, v);
                }
            });
            cachedPriceList.clear();
        } else if ("cancel".equalsIgnoreCase(command)) {
            cachedPriceList = null;
        }
        log.info("Latest price list: ");
        originalPriceList.forEach((k, v) -> {
            log.info("Value of key " + k + " is " + v);
        });
        activeBatchIDs.remove(batchId);
        reentrantLock.unlock();
    }

    /***
     * getLatestPriceOfInstrument method is used to access the latest price of the instrument id
     * This method is used by the consumer threads.
     * @param consumerThreadName - consumer name
     * @param id - instrument id to be accessed.
     * @return
     */
    public String getLatestPriceOfInstrument(String consumerThreadName, String id) throws PriceAccessException {
        log.info("Consumer "+consumerThreadName+" is trying to access price of the instrument id: "+id);
        if(activeBatchIDs.size() > 0){
            throw new PriceAccessException("Batch run is still under process. Please try accessing the value after sometime.");
        }

        return String.valueOf(originalPriceList.get(id).getPayload().get("Price").asDouble());
    }
}
