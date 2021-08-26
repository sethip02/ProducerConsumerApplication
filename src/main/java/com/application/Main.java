package com.application;

import com.application.consumer.ConsumerThread;
import com.application.producer.ProducerThread;
import com.application.service.ApplicationService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {
    public static void main(String[] args) {
        ApplicationService applicationService = new ApplicationService();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<String> result = executorService.submit(new ProducerThread(applicationService));
        while(true){
            if(result.isDone()){
                break;
            }
        }
        executorService.submit(new ConsumerThread(applicationService));
        executorService.shutdown();
        try {
            System.out.println("Result of producer thread: "+result.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
