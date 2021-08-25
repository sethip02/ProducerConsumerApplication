package com.application.producer;

import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.service.ApplicationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.internal.jline.internal.Log;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;

@Slf4j
public class ProducerThread implements Runnable{
    ApplicationService applicationService;
    UUID batchID;
    String  producerThreadName = "Producer " + Thread.currentThread().getId();
    int numOfChunksToUpload = (new Random()).nextInt(10)+ 1;
    Random priceGenerator = new Random();

    public ProducerThread(ApplicationService s){
        this.applicationService = s;
    }

    @Override
    public void run(){
            try{
                batchID = applicationService.startBatchRun();
            } catch (InterruptedException e) {
                Log.error("Producer thread "+ producerThreadName+" interuppted while starting the batch run.");
            }

            Log.info("Number of chunks to be uploaded: "+numOfChunksToUpload);
            //start uploading the chunks
            while(numOfChunksToUpload > 0){
                Chunk chunk = generateChunk(10);
                //If error occurs while generating chunks , then producer will stop uploading
                if(Objects.isNull(chunk)){
                    applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "cancel");
                    Thread.currentThread().interrupt();
                }
                applicationService.uploadChunkOfRecords(batchID, chunk);
                numOfChunksToUpload--;
            }

            //After uploading all the chunks, mark the batch run as complete
            applicationService.completeOrCancelBatchRun(batchID, producerThreadName, "complete");

    }

    private Chunk generateChunk(int size)  {
        String json = "";
        ObjectMapper objectMapper = new ObjectMapper();
        List<Record> recordList = new ArrayList<>();
        while(size > 0){
            json = "{ \"price\" :"+ priceGenerator.nextDouble() * 100 +" }";
            try {
                recordList.add(new Record(String.valueOf((new Random()).nextInt(10) + 1), LocalDateTime.now(), objectMapper.readTree(json)));
            } catch (JsonProcessingException e) {
                Log.error("Issue with the chunk generation: "+e.getMessage());
                return null;
            }
        }
        Log.info("Generated Chunk :");
        recordList.forEach((r) -> System.out.println(r));
        return new Chunk(recordList);
    }
}
