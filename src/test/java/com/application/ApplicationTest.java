package com.application;

import com.application.consumer.ConsumerThread;
import com.application.producer.ProducerThread;
import com.application.service.ApplicationService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.*;

public class ApplicationTest {
    static ApplicationService applicationService;

    @BeforeClass
    public static void initializeTests(){
        applicationService = new ApplicationService();
    }

    @Test
    public void successScenarioTest() throws ExecutionException, InterruptedException {
        //One producer and one consumer thread
        //Producer thread producing the chunks and
        // Consumer thread getting the latest value
        // of the instrument during and after the batch run
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ProducerThread producerThread = new ProducerThread(applicationService);
        ConsumerThread consumerThread = new ConsumerThread(applicationService);

        Future<String> result = executorService.submit(producerThread);
        executorService.submit(consumerThread);

        if("Batch Complete".equals(result.get())){
            executorService.submit(consumerThread);
        }

        executorService.shutdown();


    }
}
