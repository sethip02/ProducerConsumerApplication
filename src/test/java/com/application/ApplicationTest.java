package com.application;

import com.application.consumer.ConsumerThread;
import com.application.producer.ProducerThread;
import com.application.service.ApplicationService;
import lombok.extern.slf4j.Slf4j;
import nl.altindag.log.LogCaptor;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ApplicationTest {
    static ApplicationService applicationService;
    static Random random;
    LogCaptor logCaptor = LogCaptor.forClass(ProducerThread.class);

    /***
     * initializeTests method create an instance of Random and ApplicationService class to be
     * reused across all the junit tests
     */
    @BeforeClass
    public static void initializeTests() {
        random = new Random();
        applicationService = new ApplicationService();
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

        int numOfChunksToUpload = (random.nextInt(10) + 1) * 10;
        int randomInstrumentId = (random.nextInt(10)) + 1;
        log.info("Number of chunks to be uploaded for successScenarioTest: " + numOfChunksToUpload);
        log.info("Latest price to be accessed for : " + randomInstrumentId);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread = new ProducerThread("Producer1", numOfChunksToUpload, 20, 2, applicationService, false);
        ConsumerThread consumerThread = new ConsumerThread(randomInstrumentId, applicationService);

        Future<String> producerResult = executorService.submit(producerThread);
        while (true) {
            if (producerResult.isDone())
                break;
        }
        if ("Batch Complete".equals(producerResult.get())) {
            Future<String> consumerResult = consumerResult = executorService.submit(consumerThread);
            while (true) {
                if (consumerResult.isDone())
                    break;
            }
            String str = consumerResult.get();
            assertTrue(Objects.nonNull(str));
        }


        executorService.shutdown();


    }

    /***
     * Test Description: Multiple producer tries to start a batch run but
     * only one should succeed and other should wait for previous batch run to complete.
     * @throws InterruptedException
     */
    @Test
    public void multipleProducerScenarioTest() throws InterruptedException {

        int numOfChunksToUpload = (random.nextInt(10) + 1) * 10;
        log.info("Number of chunks to be uploaded by first producer: " + numOfChunksToUpload);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread1 = new ProducerThread("Producer1", numOfChunksToUpload, 10, 0, applicationService, false);

        log.info("Number of chunks to be uploaded by second producer: " + 10);
        ProducerThread producerThread2 = new ProducerThread("Producer2", 10, 10, 0, applicationService, false);
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
    public void callServiceMethodInWrongOrderTest() throws ExecutionException, InterruptedException {
        int numOfChunksToUpload = (random.nextInt(10) + 1) * 10;
        log.info("Number of chunks to be uploaded by producer: " + numOfChunksToUpload);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ProducerThread producerThread = new ProducerThread("Producer1", numOfChunksToUpload, 20, 5, applicationService, true);
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
    public void multipleProducerAndMultipleConsumerScenarioTest() throws InterruptedException {
        log.info("Number of chunks to be uploaded by producer: " + 100);
        ExecutorService producerES = Executors.newFixedThreadPool(2);
        ProducerThread producerThread1 = new ProducerThread("Producer1", 10, 100, 5, applicationService, false);
        Future<String> producerResult1 = producerES.submit(producerThread1);
        ProducerThread producerThread2 = new ProducerThread("Producer2", 10, 100, 5, applicationService, false);
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
        while (num > 0) {
            threads.add(new ConsumerThread((random.nextInt(10)) + 1, applicationService));
            num--;
        }
        return threads;
    }

}
