package com.application.consumer;

import com.application.service.ApplicationService;
import jdk.internal.jline.internal.Log;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

@Slf4j
public class ConsumerThread implements Runnable{
    ApplicationService applicationService;
    String  consumerThreadName = "Consumer " + Thread.currentThread().getId();

    public  ConsumerThread(ApplicationService applicationService){
        this.applicationService = applicationService;
    }

    @Override
    public void run() {
        int randomInstrumentId = (new Random()).nextInt(10) + 1;

        Log.info("Accessing the id: "+ randomInstrumentId);
        LocalDateTime start = LocalDateTime.now();
        Long instrumentPrice = applicationService.getLatestPriceOfInstrument(consumerThreadName, String.valueOf(randomInstrumentId));
        LocalDateTime end = LocalDateTime.now();
        Log.info("Latest price for instrument id "+ randomInstrumentId + " is "+ instrumentPrice);
        Log.info("Consumer "+consumerThreadName+" is able to access the price in "+ Duration.between(start, end).toMillis() + " ms.");
    }
}
