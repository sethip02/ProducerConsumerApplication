package com.application.service;

import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.exception.PriceAccessException;
import com.application.exception.UploadException;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/***
 * ApplicationService represents the service which offers batch upload functionality to the producers
 * and maintains the latest price of all the instruments to be made available for consumers.
 */
@Slf4j
public class ApplicationService {
    private static final ReentrantLock reentrantLock = new ReentrantLock(true);
    private static final List<String> activeBatchIDs = new ArrayList<>();
    private static Map<String, Record> originalPriceList = new ConcurrentHashMap<>();
    private final RecordComparator recordComparator = new RecordComparator();
    private Map<String, Record> cachedPriceList = new HashMap<>();

    /***
     * resetPriceList is a utility function which provides the functionality of
     * reseting the originalPriceList for evey JUnit Test
     */
    public static void resetPriceList() {
        originalPriceList = new ConcurrentHashMap<>();
    }


    /***
     * startBatchRun method is used by the producer thread to start a batch.
     * Multiple producers can start the batch.
     * All active batch ids are maintained in the static variable named activeBatchIDs
     * @param producerName - producer name which tries to start the batch run
     * @return UUID
     */
    public UUID startBatchRun(String producerName) {
        log.info(producerName + " is starting a batch run.");
        UUID newBatchID = UUID.randomUUID();
        activeBatchIDs.add(newBatchID.toString());
        log.debug("Size : " + activeBatchIDs.size());
        return newBatchID;
    }


    /***
     * uploadChunkOfRecords method is used to upload a chunk of records
     * Method will maintain a cached list of latest prices of the instruments after applying the record updates.
     * Original instrument price list remains untouched.
     * Every producer will maintain its own cachedPriceList for the duration of batch run.
     * @param batchId - batch id under which particular chunk is processed
     * @param chunk - list of records.
     */
    public void uploadChunkOfRecords(UUID batchId, Chunk chunk) {

        log.debug("Upload chunk for batch run: " + batchId.toString());
        //Check if the batch id is among the active ones. If not, throw UploadException
        if (!(activeBatchIDs.contains(batchId.toString()))) {
            throw new UploadException("Batch run with the id " + batchId + " is not active. Please check the UUID provided");
        }

        //Start processing each chunk
        List<Record> recordsToBeProcessed = chunk.getData();
        Map<String, List<Record>> groupedRecords = recordsToBeProcessed.stream().collect(Collectors.groupingBy(Record::getId));
        groupedRecords.forEach((s, l) -> {
            l.sort(recordComparator);
            Record latestRecord = l.get(0);
            cachedPriceList.put(latestRecord.getId(), latestRecord);

        });

        //Log successful upload
        log.info("Chunk uploaded successfully");

    }

    /***
     * completeOrCancelBatchRun method is used to complete or cancel a batch run
     * based on the outcome of the all the chunks upload.
     * For complete request, the cached price list is merged with the original price list.
     * Lock is acquired to update the original price list so that only one producer can update the original price list at a time.
     * For cancel request , the cached price list is discarded with no change to the original price list.
     * @param batchId - batch id
     * @param producerThreadName - producer name
     * @param command - "complete" or "cancel"
     */
    public void completeOrCancelBatchRun(UUID batchId, String producerThreadName, String command) {

        log.debug("Producer Name: " + producerThreadName + " ; " + " Command: " + command);
        //Check if the batch id is among the active ones. If not, throw UploadException
        if (!(activeBatchIDs.contains(batchId.toString()))) {
            throw new UploadException("Batch run with the id " + batchId + " is not active. Please check the UUID provided");
        }

        //Acquire a lock to update the original price list.
        reentrantLock.lock();

        if ("complete".equalsIgnoreCase(command)) {
            //copy the updates from cached list to original list
            //While uploading , the asOf field is compared to decide on whether the list have to be updated or not.
            cachedPriceList.forEach((k, v) -> {
                Record originalRecord = originalPriceList.get(k);
                if (originalRecord != null) {
                    if (v.getAsOf().compareTo(originalRecord.getAsOf()) >= 0) {
                        originalPriceList.put(k, v);
                    }
                } else {
                    originalPriceList.put(k, v);
                }
            });

            //clear the cachedPriceList
            cachedPriceList.clear();

        } else if ("cancel".equalsIgnoreCase(command)) {
            //For batch cancel, just discard the cachedPriceList.
            cachedPriceList = null;

        }

        log.debug("Latest price list: ");
        originalPriceList.forEach((k, v) -> log.debug("Value of key " + k + " is " + v));

        //Remove batch Id from activeBatchIDs list.
        activeBatchIDs.remove(batchId.toString());

        //Release the lock
        reentrantLock.unlock();
    }


    /***
     * getLatestPriceOfInstrument method is used to access the latest price of the instrument id
     * This method is used by the consumer threads.
     * @param consumerThreadName - consumer name
     * @param id - instrument id to be accessed.
     * @return InstrumentId price
     */
    public String getLatestPriceOfInstrument(String consumerThreadName, String id) throws PriceAccessException {

        log.debug("Consumer " + consumerThreadName + " is trying to access price of the instrument id: " + id);
        log.debug("Number of active batches: " + activeBatchIDs.size());

        //Check if there are any active batch runs. If yes, then throw exception
        if (activeBatchIDs.size() > 0) {
            throw new PriceAccessException("Batch run is still under process. Please try accessing the value after sometime.");
        }

        return String.valueOf(originalPriceList.get(id).getPayload().get("Price").asDouble());
    }
}
