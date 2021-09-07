package com.application.producer;

import com.application.constants.ProducerConstants;
import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.exception.UploadException;
import com.application.service.ApplicationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Callable;

public class ProducerThread implements Callable<String> {

    private static final Logger log = LoggerFactory.getLogger(ProducerThread.class);
    private ApplicationService applicationService;
    private UUID batchID = UUID.randomUUID();
    private List<Chunk> chunkList ;
    private String  producerThreadName;
    private int waitTimeToAquireLock;
    private boolean uploadChunksWithoutStart = false;
    private int numOfLockAquireRetries;

    /***
     * ProducerThread constructor takes in producer name, number of chunks to be upload as part of the batch,
     * number of records in each chunk,
     * Time (in seconds) for which producer thread waits to acquire a lock before retrying after 500 ms,
     * ApplicationService instance and boolean flag to specify if uploading to be performed without starting a batch run( introduced to simulate the
     * scenario where service methods are called in wrong order).
     * @param threadName
     * @param waitTimeToAquireLock
     * @param s
     * @param uploadChunksWithoutStart
     */
    public ProducerThread(String threadName, List<Chunk> chunkList, int waitTimeToAquireLock, int numOfRetries, ApplicationService s, boolean uploadChunksWithoutStart){
        this.producerThreadName = threadName;
        this.applicationService = s;
        this.chunkList = chunkList;
        this.waitTimeToAquireLock = waitTimeToAquireLock;
        this.uploadChunksWithoutStart = uploadChunksWithoutStart;
        this.numOfLockAquireRetries = numOfRetries;
    }

    /***
     * call method starts a batch run. And when the producer succeeds in starting the batch,
     * it uploads all the chunks of records and then completes or cancels a batch based on upload outcome.
     * Number of retries to start a batch run is hardcoded to 5 as of now.
     * And after each unsuccessful batch run start request, producer thread waits for 500ms before retrying.
     * @return batch complete or cancel outcome as a string.
     */
    @Override
    public String call(){
            int  numOfChunksToUpload = chunkList.size();
            if(uploadChunksWithoutStart == false) {
                    try {
                        int numberOfRetries = numOfLockAquireRetries;
                        while (numberOfRetries > 0) {
                            batchID = applicationService.startBatchRun(producerThreadName);
                            if (Objects.isNull(batchID)) {
                                log.error(producerThreadName + " has failed to start the batch run.It will wait for 1 second before retrying.");
                            } else {
                                break;
                            }
                            Thread.sleep(500);
                            numberOfRetries--;
                        }
                    } catch (InterruptedException e) {
                        log.error("Producer thread " + producerThreadName + " interrupted while starting the batch run.");
                        return "Batch run cannot be started due to an error: "+e.getMessage();
                    }
                log.info(producerThreadName + " started the batch run with the batchID: " + batchID);
                log.info("Number of chunks to be uploaded: "+numOfChunksToUpload);
            }

            int counter = 0;
            //start uploading the chunks
            try {
                    while(counter < numOfChunksToUpload){
                        Chunk chunk = chunkList.get(counter);
                        applicationService.uploadChunkOfRecords(batchID, chunk );
                        counter++;
                    }
            }catch(UploadException ex){
                applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "cancel");
                applicationService = null;
                return "Batch Cancelled due to an error : "+ex.getMessage();
            }

            //After uploading all the chunks, mark the batch run as complete
            applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "complete");
            applicationService = null;
            return ProducerConstants.BATCH_COMPLETION_MESSAGE;

    }


}
