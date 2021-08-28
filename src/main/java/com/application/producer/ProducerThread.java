package com.application.producer;

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
    ApplicationService applicationService;
    UUID batchID = UUID.randomUUID();
    String  producerThreadName;
    int numOfChunksToUpload;
    int waitTimeToAquireLock;
    boolean uploadChunksWithoutStart = false;
    Random priceGenerator = new Random();
    int numOfRecordsInAChunk;

    public ProducerThread(String threadName, int numOfChunksToUpload, int numOfRecordsInAChunk, int waitTimeToAquireLock, ApplicationService s, boolean uploadChunksWithoutStart){
        this.producerThreadName = threadName;
        this.numOfChunksToUpload = numOfChunksToUpload;
        this.applicationService = s;
        this.waitTimeToAquireLock = waitTimeToAquireLock;
        this.uploadChunksWithoutStart = uploadChunksWithoutStart;
        this.numOfRecordsInAChunk = numOfRecordsInAChunk;
    }

    @Override
    public String call(){
            if(uploadChunksWithoutStart == false) {
                    try {
                        int numberOfRetries = 5;
                        while (numberOfRetries > 0) {
                            batchID = applicationService.startBatchRun(producerThreadName, waitTimeToAquireLock);
                            if (Objects.isNull(batchID)) {
                                log.error(producerThreadName + " has failed to start the batch run.It will wait for 1 second before retrying.");
                            } else {
                                break;
                            }
                            Thread.sleep(1000);
                            numberOfRetries--;
                        }
                    } catch (InterruptedException e) {
                        log.error("Producer thread " + producerThreadName + " interuppted while starting the batch run.");
                    }
                log.info(producerThreadName + " started the batch run");
                log.info("Number of chunks to be uploaded: "+numOfChunksToUpload);
            }

            //start uploading the chunks
            while(numOfChunksToUpload > 0){
                Chunk chunk = generateChunk(numOfRecordsInAChunk);
                //If error occurs while generating chunks , then producer will stop uploading
                if(Objects.isNull(chunk)){
                    applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "cancel");
                    return "Batch Cancelled";
                }
                try {
                    applicationService.uploadChunkOfRecords(batchID, chunk);
                }catch(UploadException ex){
                    return "Batch Cancelled due to the error : "+ex.getMessage();
                }
                numOfChunksToUpload--;
            }

            //After uploading all the chunks, mark the batch run as complete
            applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "complete");
            return "Batch Completed";

    }

    private Chunk generateChunk(int size)  {
        Random r = new Random();
        String json = "";
        ObjectMapper objectMapper = new ObjectMapper();
        List<Record> recordList = new ArrayList<>();
        while(size > 0){
            json = "{ \"Price\" :"+ priceGenerator.nextDouble() * 100 +" }";
            try {
                recordList.add(new Record(String.valueOf(r.nextInt(10) + 1), LocalDateTime.now(), objectMapper.readTree(json)));
            } catch (JsonProcessingException e) {
                log.error("Issue with the chunk generation: "+e.getMessage());
                return null;
            }
            size--;
        }
        log.info("Generated Chunk :");
        recordList.forEach((record) -> log.info(record.toString()));
        return new Chunk(recordList);
    }
}
