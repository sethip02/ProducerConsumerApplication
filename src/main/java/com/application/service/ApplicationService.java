package com.application.service;

import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.exception.UploadException;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    static ReentrantLock reentrantLock = null;
    private UUID activeBatchUUID = null;
    RecordComparator recordComparator = new RecordComparator();
    static Map<String, Double> originalPriceList = new ConcurrentHashMap<>();
    static Map<String, Double> cachedPriceList = new HashMap<>();

    /***
     * Constructor initializes the lock for batch run
     * Only one producer should be able to acquire the lock to
     * start a batch run.
     */

    public ApplicationService() {
        reentrantLock = new ReentrantLock(true);
    }

    /***
     * startBatchRun method is used by the producer thread to start a batch
     * A lock is acquired before starting a batch run.
     * @param producerName - producer name which tries to start the batch run
     * @param waitTimeForLock - time in seconds for producer to wait for trying to acquire the lock
     * @return
     * @throws InterruptedException
     */
    public UUID startBatchRun(String producerName, int waitTimeForLock) throws InterruptedException {
        log.info(producerName + " is trying to acquire lock.");
        if (reentrantLock.tryLock(waitTimeForLock, TimeUnit.SECONDS)) {
            activeBatchUUID = UUID.randomUUID();
            return activeBatchUUID;
        } else {
            return null;

        }


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
        reentrantLock.lock();
        if (activeBatchUUID != batchId)
            throw new UploadException("Batch run with the id " + batchId + "is not active. Please check the UUID provided");


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
    public String completeOrCancelBatchRun(UUID batchId, String producerThreadName, String command) {

        if (activeBatchUUID != batchId)
            throw new UploadException("Batch run with the id " + batchId + "is not active. Please check the UUID provided");

        if ("complete".equalsIgnoreCase(command)) {
            //copy the updates from cached list to original list
            cachedPriceList.forEach((k, v) -> {
                originalPriceList.put(k, v);
            });
            cachedPriceList.clear();
        } else if ("cancel".equalsIgnoreCase(command)) {
            cachedPriceList = null;
        }

        log.info("Latest price list: ");
        originalPriceList.forEach((k, v) -> {
            log.info("Value of key " + k + " is " + v);
        });
        activeBatchUUID = null;
        reentrantLock.unlock();
        return null;
    }

    /***
     * getLatestPriceOfInstrument method is used to access the latest price of the instrument id
     * This method is used by the consumer threads.
     * @param consumerThreadName - consumer name
     * @param id - instrument id to be accessed.
     * @return
     */
    public Double getLatestPriceOfInstrument(String consumerThreadName, String id) {
        log.info("Consumer "+consumerThreadName+" is trying to access price of the instrument id: "+id);
        return originalPriceList.get(id);
    }
}
