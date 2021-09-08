package com.application;

import com.application.constants.ProducerConstants;
import com.application.consumer.ConsumerThread;
import com.application.domain.Chunk;
import com.application.domain.Record;
import com.application.domain.RecordComparator;
import com.application.exception.UploadException;
import com.application.producer.BadProducerThread;
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
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ApplicationTest {
    private static Random random = new Random();
    private LogCaptor logCaptor = LogCaptor.forClass(ProducerThread.class);
    private RecordComparator recordComparator = new RecordComparator();

    private Random priceGenerator = new Random();


    @Before
    public void resetTheLatestPriceListBeforeEveryTest(){
        ApplicationService.resetPriceList();
    }

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
        ProducerThread producerThread = new ProducerThread("Producer1", chunkList, 20, 2,new ApplicationService());
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
    public void multipleProducerScenarioTest() throws InterruptedException, JsonProcessingException, ExecutionException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Chunk> chunkList1 = new ArrayList<>();
        Chunk chunk = new Chunk();
        chunk.setData(Arrays.asList(objectMapper.readValue("{\"id\":\"1\", \"asOf\":\"2021-09-07T21:26:57.202898400\", \"payload\":{\"Price\":20.139961889377744}}",Record.class),
                        objectMapper.readValue("{\"id\":\"2\", \"asOf\":\"2021-09-07T09:26:57.265399700\", \"payload\":{\"Price\":70.14384608537513}}",Record.class)));
        chunkList1.add(chunk);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread1 = new ProducerThread("Producer1", chunkList1, 10, 4, new ApplicationService());

        Future<String> producerResult = executorService.submit(producerThread1);
        while (true) {
            if (producerResult.isDone())
                break;
        }
        if (ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult.get())) {
            List<Chunk> chunkList2 = new ArrayList<>();
            chunk.setData(Arrays.asList(objectMapper.readValue("{\"id\":\"1\", \"asOf\":\"2021-09-07T21:55:57.265399700\", \"payload\":{\"Price\":98.12933222921298}}",Record.class),
                    objectMapper.readValue("{\"id\":\"2\", \"asOf\":\"2021-09-06T10:26:57.265399700\", \"payload\":{\"Price\":31.54355122981366}}",Record.class)));
            chunkList2.add(chunk);
            ProducerThread producerThread2 = new ProducerThread("Producer2", chunkList2, 10, 4, new ApplicationService());

            producerResult = executorService.submit(producerThread2);
            while (true) {
                if (producerResult.isDone())
                    break;
            }

            if (ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult.get())) {
                Future<String> priceFromConsumer1 = executorService.submit(new ConsumerThread("Consumer1", 1, new ApplicationService()));
                Future<String> priceFromConsumer2 = executorService.submit(new ConsumerThread("Consumer2", 2, new ApplicationService()));

                assertEquals("98.12933222921298",priceFromConsumer1.get());
                assertEquals("70.14384608537513",priceFromConsumer2.get());
            }
        }

        executorService.shutdown();

    }

    /***
     * Test Description: Producer tries to upload the chunks before starting a batch run.
     * Producer should not be able to upload the chunk without first starting the batch.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test(expected = ExecutionException.class)
    public void callServiceMethodInWrongOrderTest() throws ExecutionException, InterruptedException {
        List<Chunk> chunkList = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 5;
        while(numOfChunksToUpload > 0){
            chunkList.add(generateChunk(10));
            numOfChunksToUpload--;
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        BadProducerThread producerThread = new BadProducerThread("Producer1", chunkList, 20, 5, new ApplicationService());
        Future<String> producerResult = executorService.submit(producerThread);
        while (true) {
            if (producerResult.isDone())
                break;
        }
        log.info("Result : " + producerResult.get());
        executorService.shutdown();



    }

    @Test
    public void testConsumerAccessExceptionDuringBatchRun() throws ExecutionException, InterruptedException {
        List<Chunk> chunkList = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 1000;
        while(numOfChunksToUpload > 0){
            chunkList.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread = new ProducerThread("Producer1", chunkList, 20, 5, new ApplicationService());
        Future<String> producerResult = executorService.submit(producerThread);
        Thread.sleep(10);
        Future<String> result = executorService.submit(new ConsumerThread("Consumer1", 1, new ApplicationService()));
        //log.info("Price value: "+priceFromConsumer.get());
        assertEquals(null,result.get());
        log.info("Producer result:"+producerResult.get());

    }

    /***
     * Test Description: Multiple producers and consumers thread are spun.
     * Only one producer should be able to start the batch run and others should wait for batch run to complete.
     * Consumers should be able to access the latest price of the instrument id without any delay.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void multipleProducerAndMultipleConsumerScenarioTest() throws InterruptedException, ExecutionException {
        List<Chunk> chunkList1 = new ArrayList<>();
        //generate chunks
        int numOfChunksToUpload = 1000;
        while(numOfChunksToUpload > 0){
            chunkList1.add(generateChunk(10));
            numOfChunksToUpload--;
        }


        Map<String, Record> expectedPriceList = new HashMap<>();
        String expectedPrice = "";
        chunkList1.forEach((chunk) -> {
            Map<String, List<Record>> groupedRecords = chunk.getData().stream().collect(Collectors.groupingBy(Record::getId));
            groupedRecords.forEach((s, l) -> {
                l.sort(recordComparator);
                Record latestRecord = l.get(0);
                expectedPriceList.put(latestRecord.getId(), latestRecord);

            });

        });


        List<Chunk> chunkList2 = new ArrayList<>();
        //generate chunks
        numOfChunksToUpload = 1000;
        while(numOfChunksToUpload > 0){
            chunkList2.add(generateChunk(10));
            numOfChunksToUpload--;
        }

        //Calculate expected result
        Map<String, Record> cachedPriceList = new HashMap<>();
        chunkList2.forEach((chunk) -> {
            Map<String, List<Record>> groupedRecords = chunk.getData().stream().collect(Collectors.groupingBy(Record::getId));
            groupedRecords.forEach((s, l) -> {
                l.sort(recordComparator);
                Record latestRecord = l.get(0);
                cachedPriceList.put(latestRecord.getId(), latestRecord);

            });

        });

        //calculating expected price
        cachedPriceList.forEach((k, v) -> {
            Record originalRecord = expectedPriceList.get(k);
            if(originalRecord != null) {
                if (v.getAsOf().compareTo(originalRecord.getAsOf()) >= 0) {
                    expectedPriceList.put(k, v);
                }
            }else{
                expectedPriceList.put(k, v);
            }
        });

        expectedPrice = String.valueOf(expectedPriceList.get("5").getPayload().get("Price"));
        log.info("Expected latest price: "+expectedPrice);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        ProducerThread producerThread1 = new ProducerThread("Producer1", chunkList1, 100, 5, new ApplicationService());
        Future<String> producerResult1 = executorService.submit(producerThread1);


        ProducerThread producerThread2 = new ProducerThread("Producer2", chunkList2, 100, 5, new ApplicationService());
        Future<String> producerResult2 = executorService.submit(producerThread2);

        Thread.sleep(20);
        Future<String> consumer1Result = executorService.submit(new ConsumerThread("Consumer1", 1, new ApplicationService()));
        assertEquals(null,consumer1Result.get());

        //Wait for all the batch runs to be completed
        if(ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult1.get()) && ProducerConstants.BATCH_COMPLETION_MESSAGE.equals(producerResult2.get())){
            Future<String> consumer2Result = executorService.submit(new ConsumerThread("Consumer2", 5, new ApplicationService()));
            assertEquals(expectedPrice, consumer2Result.get());
        }
        executorService.shutdown();

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
