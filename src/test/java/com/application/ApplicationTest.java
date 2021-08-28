package com.application;

import com.application.consumer.ConsumerThread;
import com.application.producer.ProducerThread;
import com.application.service.ApplicationService;
import static  org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import lombok.extern.slf4j.Slf4j;
import nl.altindag.log.LogCaptor;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class ApplicationTest {
    static ApplicationService applicationService;
    static Random random;
    LogCaptor logCaptor = LogCaptor.forClass(ProducerThread.class);

    @BeforeClass
    public static void initializeTests(){
        random = new Random();
        applicationService = new ApplicationService();
    }

    @Test
    @Ignore
    public void successScenarioTest() throws ExecutionException, InterruptedException {
        //One producer and one consumer thread
        //Producer thread producing the chunks and
        // Consumer thread getting the latest value
        // of the instrument during and after the batch run

        int numOfChunksToUpload = (random.nextInt(10)+ 1)*10;
        int randomInstrumentId = (random.nextInt(10)) + 1;
        log.info("Number of chunks to be uploaded for successScenarioTest: "+numOfChunksToUpload);
        log.info("Latest price to be accessed for : " + randomInstrumentId);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread = new ProducerThread("Producer1" , numOfChunksToUpload, 20, 2, applicationService, false);
        ConsumerThread consumerThread = new ConsumerThread(randomInstrumentId, applicationService);

        Future<String> producerResult = executorService.submit(producerThread);
        while(true){
            if(producerResult.isDone())
                break;
        }
        if("Batch Complete".equals(producerResult.get())){
            Future<String> consumerResult = consumerResult = executorService.submit(consumerThread);
            while(true){
                if(consumerResult.isDone())
                    break;
            }
            String str = consumerResult.get();
            assertTrue(Objects.nonNull(str));
        }


        executorService.shutdown();


    }

    @Test
    public void multipleProducerScenarioTest() throws InterruptedException {
        //Multiple producer trying to start a batch run
        //Only one should succeed and other should wait for previous batch run to complete.
        int numOfChunksToUpload = (random.nextInt(10)+ 1)*10;
        log.info("Number of chunks to be uploaded by first producer: "+numOfChunksToUpload);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread1 = new ProducerThread("Producer1", numOfChunksToUpload, 20, 5, applicationService, false);
        numOfChunksToUpload = (random.nextInt(10)+ 1);
        log.info("Number of chunks to be uploaded by second producer: "+numOfChunksToUpload);
        ProducerThread producerThread2 = new ProducerThread("Producer2",numOfChunksToUpload, 20, 1, applicationService, false);
        List<ProducerThread> threadList = Arrays.asList(producerThread1, producerThread2);
        List<Future<String>> futureList = executorService.invokeAll(threadList);
        while(true){
            if(futureList.get(0).isDone() || futureList.get(1).isDone())
                break;
        }
        assertThat(logCaptor.getErrorLogs()).contains("Producer2 has failed to start the batch run.It will wait for 1 second before retrying.");
        log.info("Multiple producer scenario completed successfully");
        executorService.shutdown();

    }

    @Test
    public void callServiceMethodInWrongOrderTest() throws ExecutionException, InterruptedException {
        int numOfChunksToUpload = (random.nextInt(10)+ 1)*10;
        log.info("Number of chunks to be uploaded by producer: "+numOfChunksToUpload);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ProducerThread producerThread = new ProducerThread("Producer1", numOfChunksToUpload, 20, 5, applicationService, true);
        Future<String> producerResult = executorService.submit(producerThread);
        while(true){
            if(producerResult.isDone())
                break;
        }
        log.info("Result : "+producerResult.get());
        assertTrue(producerResult.get().contains("Batch Cancelled due to the error"));

    }

    @Test
    public void singleProducerAndMultipleConsumerScenarioTest() throws InterruptedException {
        log.info("Number of chunks to be uploaded by producer: "+100);
        ExecutorService producerES = Executors.newSingleThreadExecutor();
        ProducerThread producerThread = new ProducerThread("Producer1", 10, 100, 5, applicationService, false);
        Future<String> producerResult = producerES.submit(producerThread);
        while(true){
            if(producerResult.isDone())
                break;
        }

        ExecutorService consumerES = Executors.newFixedThreadPool(10);
        int numOfAccess = 50;
        while(numOfAccess > 0){
            //generate consumer threads.
            consumerES.invokeAll(generateConsumerThreads((random.nextInt(10)) + 1));
            Thread.sleep(1000);
            numOfAccess--;
        }

    }

    public List<ConsumerThread> generateConsumerThreads(int num){
        List<ConsumerThread> threads = new ArrayList<>();
        while(num > 0){
            threads.add(new ConsumerThread((random.nextInt(10)) + 1, applicationService));
            num--;
        }
        return threads;
    }

}
