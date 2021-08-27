package com.application.producer;

import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.service.ApplicationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Callable;

@Slf4j
public class ProducerThread implements Callable<String> {
    ApplicationService applicationService;
    UUID batchID;
    String  producerThreadName = "Producer " + Thread.currentThread().getId();
    int numOfChunksToUpload;
    Random priceGenerator = new Random();

    public ProducerThread(int numOfChunksToUpload, ApplicationService s){
        this.numOfChunksToUpload = numOfChunksToUpload;
        this.applicationService = s;
    }

    @Override
    public String call(){
            try{
                batchID = applicationService.startBatchRun(producerThreadName);
            } catch (InterruptedException e) {
                log.error("Producer thread "+ producerThreadName+" interuppted while starting the batch run.");
            }

        log.info("Number of chunks to be uploaded: "+numOfChunksToUpload);
            //start uploading the chunks
            while(numOfChunksToUpload > 0){
                Chunk chunk = generateChunk(20);
                //If error occurs while generating chunks , then producer will stop uploading
                if(Objects.isNull(chunk)){
                    applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "cancel");
                    return "Batch Cancelled";
                }
                applicationService.uploadChunkOfRecords(batchID, chunk);
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
