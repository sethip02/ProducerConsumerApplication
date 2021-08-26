package com.application.consumer;

import com.application.service.ApplicationService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.Callable;

@Slf4j
public class ConsumerThread implements Callable {
    ApplicationService applicationService;
    String  consumerThreadName = "Consumer " + Thread.currentThread().getId();

    public  ConsumerThread(ApplicationService applicationService){
        this.applicationService = applicationService;
    }

    @Override
    public String call() {
        int randomInstrumentId = (new Random()).nextInt(10) + 1;

        log.info("Accessing the id: "+ randomInstrumentId);
        LocalDateTime start = LocalDateTime.now();
        Double instrumentPrice = applicationService.getLatestPriceOfInstrument(consumerThreadName, String.valueOf(randomInstrumentId));
        LocalDateTime end = LocalDateTime.now();
        log.info("Latest price for instrument id "+ randomInstrumentId + " is "+ instrumentPrice);
        log.info("Consumer "+consumerThreadName+" is able to access the price in "+ Duration.between(start, end).toMillis() + " ms.");
        return "Value of instrument "+randomInstrumentId +" is "+instrumentPrice;

    }
}
