package com.application.consumer;

import com.application.service.ApplicationService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

/***
 * Consumer Thread class represents the consumer of the service.
 * Constructor of the class accepts the instrument id to be accessed and application service instance.
 */
@Slf4j
public class ConsumerThread implements Callable<String> {
    ApplicationService applicationService;
    String consumerThreadName = "Consumer " + Thread.currentThread().getName();
    int randomInstrumentId;

    public ConsumerThread(int randomInstrumentId, ApplicationService applicationService) {
        this.randomInstrumentId = randomInstrumentId;
        this.applicationService = applicationService;
    }

    /***
     * The overriden call method accesses the latest price of the instrument id.
     * @return latest price of the instrument.
     */
    @Override
    public String call() {


        log.info("Accessing the id: " + randomInstrumentId);
        LocalDateTime start = LocalDateTime.now();
        Double instrumentPrice = applicationService.getLatestPriceOfInstrument(consumerThreadName, String.valueOf(randomInstrumentId));
        LocalDateTime end = LocalDateTime.now();
        log.info("Latest price for instrument id " + randomInstrumentId + " is " + instrumentPrice);
        log.info("Consumer " + consumerThreadName + " is able to access the price in " + Duration.between(start, end).toMillis() + " ms.");
        return "Value of instrument " + randomInstrumentId + " is " + instrumentPrice;

    }
}
