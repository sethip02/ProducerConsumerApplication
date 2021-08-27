package com.application;

import com.application.consumer.ConsumerThread;
import com.application.producer.ProducerThread;
import com.application.service.ApplicationService;
import static  org.junit.Assert.*;

import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.*;

@Slf4j
public class ApplicationTest {
    static ApplicationService applicationService;
    static Random random;
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
        ProducerThread producerThread = new ProducerThread(numOfChunksToUpload,applicationService);
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
        ProducerThread producerThread1 = new ProducerThread(numOfChunksToUpload,applicationService);
        numOfChunksToUpload = (random.nextInt(10)+ 1);
        log.info("Number of chunks to be uploaded by second producer: "+numOfChunksToUpload);
        ProducerThread producerThread2 = new ProducerThread(numOfChunksToUpload,applicationService);
        List<ProducerThread> threadList = Arrays.asList(producerThread1, producerThread2);
        List<Future<String>> futureList = executorService.invokeAll(threadList);
        while(true){
            if(futureList.get(0).isDone() || futureList.get(1).isDone())
                break;
        }

        log.info("Multiple producer scenario completed successfully");
        executorService.shutdown();

    }
}
