package com.application.producer;

import com.application.constants.ProducerConstants;
import com.application.domain.Chunk;
import com.application.exception.UploadException;
import com.application.service.ApplicationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;

public class ProducerThread implements Callable<String> {

    private static final Logger log = LoggerFactory.getLogger(ProducerThread.class);
    private final List<Chunk> chunkList;
    private final String producerThreadName;
    private final int numOfLockAquireRetries;
    private ApplicationService applicationService;
    private UUID batchID;

    /***
     * ProducerThread constructor takes in producer name, list of chunks to be upload as part of the batch, number of retries and
     * ApplicationService instance.
     * @param threadName name of the producer thread
     * @param chunkList list of chunks to be uploaded
     * @param numOfRetries number of retries for starting the batch run
     * @param s instance of application service for producer thread
     */
    public ProducerThread(String threadName, List<Chunk> chunkList, int numOfRetries, ApplicationService s) {
        this.producerThreadName = threadName;
        this.applicationService = s;
        this.chunkList = chunkList;
        this.numOfLockAquireRetries = numOfRetries;
    }

    /***
     * call method starts a batch run. And when the producer succeeds in starting the batch,
     * it uploads all the chunks of records and then completes or cancels a batch based on upload outcome.
     * And after each unsuccessful batch run request, producer thread waits for 500ms before retrying.
     * @return batch complete or cancel outcome as a string.
     */
    @Override
    public String call() {
        //Producer thread tries to start the batch
        int numOfChunksToUpload = chunkList.size();
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
            return "Batch run cannot be started due to an error: " + e.getMessage();
        }

        log.info(producerThreadName + " started the batch run with the batchID: " + batchID);
        log.debug("Number of chunks to be uploaded: " + numOfChunksToUpload);


        int counter = 0;
        //start uploading the chunks
        try {
            while (counter < numOfChunksToUpload) {
                Chunk chunk = chunkList.get(counter);
                applicationService.uploadChunkOfRecords(batchID, chunk);
                counter++;
            }
        } catch (UploadException ex) {
            applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "cancel");
            applicationService = null;
            return "Batch Cancelled due to an error : " + ex.getMessage();
        }

        //After uploading all the chunks, mark the batch run as complete
        applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "complete");
        //release the application service instance for garbage collection
        applicationService = null;

        //return completion message
        return ProducerConstants.BATCH_COMPLETION_MESSAGE;

    }


}
