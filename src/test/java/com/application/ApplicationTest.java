package com.application;

import com.application.constants.ProducerConstants;
import com.application.consumer.ConsumerThread;
import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.producer.ProducerThread;
import com.application.service.ApplicationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.altindag.log.LogCaptor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ApplicationTest {
    private static Random random = new Random();
    private LogCaptor logCaptor = LogCaptor.forClass(ProducerThread.class);
    private RecordComparator recordComparator = new RecordComparator();

    private Random priceGenerator = new Random();


    /***
     * Test Description: Initializes one producer and one consumer thread.
     * Producer thread produces the chunks and uploads them.
     * Consumer thread gets the latest value of the instrument after
     * the batch run is complete
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void successScenarioTest() throws ExecutionException, InterruptedException {
        List<Chunk> chunkList = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 5;
        while(numOfChunksToUpload > 0){
            chunkList.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        //Calculate expected result
        Map<String, Record> expectedPriceList = new HashMap<>();
        String expectedPrice = "";
        chunkList.forEach((chunk) -> {
            Map<String, List<Record>> groupedRecords = chunk.getData().stream().collect(Collectors.groupingBy(Record::getId));
            groupedRecords.forEach((s, l) -> {
                l.sort(recordComparator);
                Record latestRecord = l.get(0);
                expectedPriceList.put(latestRecord.getId(), latestRecord);

            });

        });
        expectedPrice = String.valueOf(expectedPriceList.get("7").getPayload().get("Price"));
        log.info("Expected latest price: "+expectedPrice);

        int instrumentId = 7;
        log.info("Latest price to be accessed for : " + instrumentId);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread = new ProducerThread("Producer1", chunkList, 20, 2,new ApplicationService(), false);
        ConsumerThread consumerThread = new ConsumerThread("Consumer1", instrumentId, new ApplicationService());

        Future<String> producerResult = executorService.submit(producerThread);
        while (true) {
            if (producerResult.isDone())
                break;
        }
        if (ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult.get())) {
            Future<String> consumerResult = executorService.submit(consumerThread);
            while (true) {
                if (consumerResult.isDone())
                    break;
            }
            String str = consumerResult.get();
            assertTrue(Double.compare(Double.parseDouble(expectedPrice), Double.parseDouble(str)) == 0);
        }


        executorService.shutdown();


    }

    /***
     * Test Description: Multiple producer tries to start a batch run but
     * only one should succeed and other should wait for previous batch run to complete.
     * @throws InterruptedException
     */
    @Test
    @Ignore
    public void multipleProducerScenarioTest() throws InterruptedException {

        List<Chunk> chunkList1 = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 5;
        while(numOfChunksToUpload > 0){
            chunkList1.add(generateChunk(10));
            numOfChunksToUpload--;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread1 = new ProducerThread("Producer1", chunkList1, 10, 0, new ApplicationService(), false);

        List<Chunk> chunkList2 = new ArrayList<>();
        //generate chunks
        numOfChunksToUpload = 5;
        while(numOfChunksToUpload > 0){
            chunkList2.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        ProducerThread producerThread2 = new ProducerThread("Producer2", chunkList2, 10, 0, new ApplicationService(), false);
        List<ProducerThread> threadList = Arrays.asList(producerThread1, producerThread2);
        List<Future<String>> futureList = executorService.invokeAll(threadList);
        while (true) {
            if (futureList.get(0).isDone() || futureList.get(1).isDone())
                break;
        }
        assertThat(logCaptor.getErrorLogs()).containsAnyOf("Producer1 has failed to start the batch run.It will wait for 1 second before retrying.", "Producer2 has failed to start the batch run.It will wait for 1 second before retrying.");
        log.info("Multiple producer scenario completed successfully");
        executorService.shutdown();

    }

    /***
     * Test Description: Producer tries to upload the chunks before starting a batch run.
     * Producer should not be able to upload the chunk without first starting the batch.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    @Ignore
    public void callServiceMethodInWrongOrderTest() throws ExecutionException, InterruptedException {
        List<Chunk> chunkList = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 5;
        while(numOfChunksToUpload > 0){
            chunkList.add(generateChunk(10));
            numOfChunksToUpload--;
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ProducerThread producerThread = new ProducerThread("Producer1", chunkList, 20, 5, new ApplicationService(), true);
        Future<String> producerResult = executorService.submit(producerThread);
        while (true) {
            if (producerResult.isDone())
                break;
        }
        log.info("Result : " + producerResult.get());
        executorService.shutdown();
        assertTrue(producerResult.get().contains("Batch Cancelled due to the error"));


    }

    /***
     * Test Description: Multiple producers and consumers thread are spun.
     * Only one producer should be able to start the batch run and others should wait for batch run to complete.
     * Consumers should be able to access the latest price of the instrument id without any delay.
     * @throws InterruptedException
     */
    @Test
    @Ignore
    public void multipleProducerAndMultipleConsumerScenarioTest() throws InterruptedException {
        List<Chunk> chunkList1 = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 5;
        while(numOfChunksToUpload > 0){
            chunkList1.add(generateChunk(10));
            numOfChunksToUpload--;
        }
        ExecutorService producerES = Executors.newFixedThreadPool(2);
        ProducerThread producerThread1 = new ProducerThread("Producer1", chunkList1, 100, 5, new ApplicationService(), false);
        Future<String> producerResult1 = producerES.submit(producerThread1);


        List<Chunk> chunkList2 = new ArrayList<>();
        //generate chunks
        numOfChunksToUpload = 5;
        while(numOfChunksToUpload > 0){
            chunkList2.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        ProducerThread producerThread2 = new ProducerThread("Producer2", chunkList2, 100, 5, new ApplicationService(), false);
        Future<String> producerResult2 = producerES.submit(producerThread2);
        ExecutorService consumerES = Executors.newFixedThreadPool(10);
        int numOfAccess = 10;
        while (numOfAccess > 0) {
            //generate consumer threads.
            consumerES.invokeAll(generateConsumerThreads((random.nextInt(10)) + 1));
            Thread.sleep(1000);
            numOfAccess--;
        }

        producerES.shutdown();
        consumerES.shutdown();

    }

    /***
     * Method to generate the list of consumer thread
     * @param num
     * @return
     */
    public List<ConsumerThread> generateConsumerThreads(int num) {
        List<ConsumerThread> threads = new ArrayList<>();
        int i = 1;
        while (i <= num) {
            threads.add(new ConsumerThread("Consumer"+i , i, new ApplicationService()));
            i++;
        }
        return threads;
    }

    /***
     * generateChunk method is a custom chunk generator which generates random list of records.
     * @param size - number of records in a chunk
     * @return
     */
    private Chunk generateChunk(int size)  {
        Random r = new Random();
        String json = "";
        ObjectMapper objectMapper = new ObjectMapper();
        List<Record> recordList = new ArrayList<>();
        int i = 1;
        while(i <= size){
            json = "{ \"Price\" :"+ priceGenerator.nextDouble() * 100 +" }";
            try {
                recordList.add(new Record(String.valueOf(i), LocalDateTime.now(), objectMapper.readTree(json)));
            } catch (JsonProcessingException e) {
                log.error("Issue with the chunk generation: "+e.getMessage());
                return null;
            }
            i++;

        }
        log.info("Generated Chunk :");
        recordList.forEach((record) -> log.info(record.toString()));
        return new Chunk(recordList);
    }

}
