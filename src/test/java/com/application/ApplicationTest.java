package com.application;

import com.application.constants.ProducerConstants;
import com.application.consumer.ConsumerThread;
import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.producer.BadProducerThread;
import com.application.producer.ProducerThread;
import com.application.service.ApplicationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Slf4j
public class ApplicationTest {

    private final RecordComparator recordComparator = new RecordComparator();
    private final Random priceGenerator = new Random();


    @Before
    public void resetTheLatestPriceListBeforeEveryTest() {
        ApplicationService.resetPriceList();
    }

    /***
     * Test Description: Initializes one producer and one consumer thread.
     * Producer thread produces the chunks and uploads them.
     * Consumer thread gets the latest value of the instrument after
     * the batch run is complete
     * @throws ExecutionException Thread execution exception
     * @throws InterruptedException thread interrupt exception
     */
    @Test
    public void successScenarioTest() throws ExecutionException, InterruptedException {
        List<Chunk> chunkList = new ArrayList<>();
        //generate 5 chunks of 10 records each
        int numOfChunksToUpload = 5;
        while (numOfChunksToUpload > 0) {
            chunkList.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        //Calculate expected result
        Map<String, Record> expectedPriceList = new HashMap<>();
        String expectedPrice;
        chunkList.forEach((chunk) -> {
            Map<String, List<Record>> groupedRecords = chunk.getData().stream().collect(Collectors.groupingBy(Record::getId));
            groupedRecords.forEach((s, l) -> {
                l.sort(recordComparator);
                Record latestRecord = l.get(0);
                expectedPriceList.put(latestRecord.getId(), latestRecord);

            });

        });
        expectedPrice = String.valueOf(expectedPriceList.get("7").getPayload().get("Price"));
        log.debug("Expected latest price: " + expectedPrice);

        //InstrumentId 7 is to be accessed by consumer thread
        int instrumentId = 7;
        log.info("Latest price to be accessed for : " + instrumentId);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread = new ProducerThread("Producer1", chunkList, 20, new ApplicationService());
        ConsumerThread consumerThread = new ConsumerThread("Consumer1", instrumentId, new ApplicationService());

        Future<String> producerResult = executorService.submit(producerThread);
        //Wait for producer thread to complete
        while (true) {
            if (producerResult.isDone())
                break;
        }

        //Check if the producer thread is complete successfully
        if (ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult.get())) {
            Future<String> consumerResult = executorService.submit(consumerThread);
            while (true) {
                if (consumerResult.isDone())
                    break;
            }
            String str = consumerResult.get();
            //Check if the expected and actual price is same
            assertEquals(0, Double.compare(Double.parseDouble(expectedPrice), Double.parseDouble(str)));
        } else {
            //If producer fails to complete batch run, then fail the test case
            Assert.fail();
        }


        executorService.shutdown();


    }

    /***
     * Test Description: Multiple producer tries to start a batch run in sequential manner.
     * Both the producer should be able to run the batch run.
     * Once both the producer threads are done with the batch run, 2 consumer threads tries to access
     * different instrument id value.
     * @throws InterruptedException thread interrupt exception
     */
    @Test
    public void multipleProducerScenarioTest() throws InterruptedException, JsonProcessingException, ExecutionException {

        //Create list of chunk for producer 1
        ObjectMapper objectMapper = new ObjectMapper();
        List<Chunk> chunkList1 = new ArrayList<>();
        Chunk chunk = new Chunk();
        chunk.setData(Arrays.asList(objectMapper.readValue("{\"id\":\"1\", \"asOf\":\"2021-09-07T21:26:57.202898400\", \"payload\":{\"Price\":20.139961889377744}}", Record.class),
                objectMapper.readValue("{\"id\":\"2\", \"asOf\":\"2021-09-07T09:26:57.265399700\", \"payload\":{\"Price\":70.14384608537513}}", Record.class)));
        chunkList1.add(chunk);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread1 = new ProducerThread("Producer1", chunkList1, 10, new ApplicationService());

        Future<String> producerResult = executorService.submit(producerThread1);
        while (true) {
            if (producerResult.isDone())
                break;
        }

        //Once the producer 1 is done with the batch run, producer 2 starts the batch run
        if (ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult.get())) {
            List<Chunk> chunkList2 = new ArrayList<>();
            chunk.setData(Arrays.asList(objectMapper.readValue("{\"id\":\"1\", \"asOf\":\"2021-09-07T21:55:57.265399700\", \"payload\":{\"Price\":98.12933222921298}}", Record.class),
                    objectMapper.readValue("{\"id\":\"2\", \"asOf\":\"2021-09-06T10:26:57.265399700\", \"payload\":{\"Price\":31.54355122981366}}", Record.class)));
            chunkList2.add(chunk);
            ProducerThread producerThread2 = new ProducerThread("Producer2", chunkList2, 10, new ApplicationService());

            producerResult = executorService.submit(producerThread2);
            while (true) {
                if (producerResult.isDone())
                    break;
            }

            //Once producer 2 is also done with the batch , then start 2 consumer threads to access the instrument id value.
            if (ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult.get())) {
                Future<String> priceFromConsumer1 = executorService.submit(new ConsumerThread("Consumer1", 1, new ApplicationService()));
                Future<String> priceFromConsumer2 = executorService.submit(new ConsumerThread("Consumer2", 2, new ApplicationService()));

                assertEquals("98.12933222921298", priceFromConsumer1.get());
                assertEquals("70.14384608537513", priceFromConsumer2.get());
            }
        }

        executorService.shutdown();

    }

    /***
     * Test Description: Producer tries to upload the chunks before starting a batch run.
     * Producer should not be able to upload the chunk without first starting the batch.
     * @throws ExecutionException Thread execution exception
     * @throws InterruptedException Thread interrupt exception
     */
    @Test(expected = ExecutionException.class)
    public void callServiceMethodInWrongOrderTest() throws ExecutionException, InterruptedException {
        List<Chunk> chunkList = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 5;
        while (numOfChunksToUpload > 0) {
            chunkList.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        //Start a bad producer thread
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        BadProducerThread producerThread = new BadProducerThread("Producer1", chunkList, 20, new ApplicationService());
        Future<String> producerResult = executorService.submit(producerThread);
        while (true) {
            if (producerResult.isDone())
                break;
        }
        log.debug("Result : " + producerResult.get());
        executorService.shutdown();


    }

    /***
     * Test description: Consumer tries to access the price of the instrument while the batch is running
     * Execution Exception wrapping the PriceAccessException is thrown
     * @throws ExecutionException Thread execution exception
     * @throws InterruptedException Thread interrupt exception
     */
    @Test
    public void testConsumerAccessExceptionDuringBatchRun() throws ExecutionException, InterruptedException {
        List<Chunk> chunkList = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 1000;
        while (numOfChunksToUpload > 0) {
            chunkList.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        //Start a producer thread
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread = new ProducerThread("Producer1", chunkList, 20, new ApplicationService());
        Future<String> producerResult = executorService.submit(producerThread);
        //Wait for batch run to start
        Thread.sleep(10);
        //Start a consumer thread in parallel
        Future<String> result = executorService.submit(new ConsumerThread("Consumer1", 1, new ApplicationService()));
        //Consumer threads gets null value of instrument id price
        assertNull(result.get());
        log.info("Producer result:" + producerResult.get());

    }

    /***
     * Test Description: Multiple producers and consumers thread are spun.
     * Producers execute the batch run in parallel. But the updates applied to original price list is done by one producer thread at a time
     * One consumer tries to access the price while the batch is running and other consumer accesses the price once all the producers are done with the
     * batch run.
     * @throws InterruptedException Thread interrupt exception
     * @throws ExecutionException Thread execution exception
     */
    @Test
    public void multipleProducerAndMultipleConsumerScenarioTest() throws InterruptedException, ExecutionException {
        List<Chunk> chunkList1 = new ArrayList<>();
        //generate chunks for producer 1
        int numOfChunksToUpload = 1000;
        while (numOfChunksToUpload > 0) {
            chunkList1.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        //Calculate the cached price list for producer 1
        Map<String, Record> expectedPriceList = new HashMap<>();
        String expectedPrice;
        chunkList1.forEach((chunk) -> {
            Map<String, List<Record>> groupedRecords = chunk.getData().stream().collect(Collectors.groupingBy(Record::getId));
            groupedRecords.forEach((s, l) -> {
                l.sort(recordComparator);
                Record latestRecord = l.get(0);
                expectedPriceList.put(latestRecord.getId(), latestRecord);

            });

        });


        List<Chunk> chunkList2 = new ArrayList<>();
        //generate chunks for producer 2
        numOfChunksToUpload = 1000;
        while (numOfChunksToUpload > 0) {
            chunkList2.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        //Calculate cached price list for producer 2
        Map<String, Record> cachedPriceList = new HashMap<>();
        chunkList2.forEach((chunk) -> {
            Map<String, List<Record>> groupedRecords = chunk.getData().stream().collect(Collectors.groupingBy(Record::getId));
            groupedRecords.forEach((s, l) -> {
                l.sort(recordComparator);
                Record latestRecord = l.get(0);
                cachedPriceList.put(latestRecord.getId(), latestRecord);

            });

        });

        //calculating expected price with both the producer's cached price list
        cachedPriceList.forEach((k, v) -> {
            Record originalRecord = expectedPriceList.get(k);
            if (originalRecord != null) {
                if (v.getAsOf().compareTo(originalRecord.getAsOf()) >= 0) {
                    expectedPriceList.put(k, v);
                }
            } else {
                expectedPriceList.put(k, v);
            }
        });

        expectedPrice = String.valueOf(expectedPriceList.get("5").getPayload().get("Price"));
        log.info("Expected latest price: " + expectedPrice);
        //Start the producer 1 thread
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        ProducerThread producerThread1 = new ProducerThread("Producer1", chunkList1, 100, new ApplicationService());
        Future<String> producerResult1 = executorService.submit(producerThread1);

        //Start the producer 2 thread
        ProducerThread producerThread2 = new ProducerThread("Producer2", chunkList2, 100, new ApplicationService());
        Future<String> producerResult2 = executorService.submit(producerThread2);

        //Wait for both the batch run to start
        Thread.sleep(20);
        //Start a consumer thread to access the price while the batch is running
        Future<String> consumer1Result = executorService.submit(new ConsumerThread("Consumer1", 1, new ApplicationService()));
        assertNull(consumer1Result.get());

        //Wait for all the batch runs to be completed
        if (ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult1.get()) && ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult2.get())) {
            //Start a consumer thread to access the price and then apply assertion to the expected and actual price
            Future<String> consumer2Result = executorService.submit(new ConsumerThread("Consumer2", 5, new ApplicationService()));
            assertEquals(expectedPrice, consumer2Result.get());
        }
        executorService.shutdown();

    }

    /***
     * generateChunk method is a custom chunk generator which generates random list of records.
     * @param size - number of records in a chunk
     * @return Chunk
     */
    private Chunk generateChunk(int size) {
        String json;
        ObjectMapper objectMapper = new ObjectMapper();
        List<Record> recordList = new ArrayList<>();
        int i = 1;
        while (i <= size) {
            json = "{ \"Price\" :" + priceGenerator.nextDouble() * 100 + " }";
            try {
                recordList.add(new Record(String.valueOf(i), LocalDateTime.now(), objectMapper.readTree(json)));
            } catch (JsonProcessingException e) {
                log.error("Issue with the chunk generation: " + e.getMessage());
                return null;
            }
            i++;

        }
        log.info("Generated Chunk :");
        recordList.forEach((record) -> log.info(record.toString()));
        return new Chunk(recordList);
    }

}
