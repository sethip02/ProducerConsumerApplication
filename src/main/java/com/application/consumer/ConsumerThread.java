package com.application.consumer;

import com.application.constants.ErrorConstants;
import com.application.exception.PriceAccessException;
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
    private ApplicationService applicationService;
    private String consumerThreadName;
    private int instrumentId;

    public ConsumerThread(String consumerThreadName, int instrumentId, ApplicationService applicationService) {
        this.consumerThreadName = consumerThreadName;
        this.instrumentId = instrumentId;
        this.applicationService = applicationService;
    }

    /***
     * The overriden call method accesses the latest price of the instrument id.
     * @return latest price of the instrument.
     */
    @Override
    public String call() {


        log.info("Accessing the id: " + instrumentId);
        LocalDateTime start = LocalDateTime.now();
        String instrumentPrice = null;
        try {
            instrumentPrice = applicationService.getLatestPriceOfInstrument(consumerThreadName, String.valueOf(instrumentId));
            LocalDateTime end = LocalDateTime.now();
            log.info("Latest price for instrument id " + instrumentId + " is " + instrumentPrice);
            log.info("Consumer " + consumerThreadName + " is able to access the price in " + Duration.between(start, end).toMillis() + " ms.");
        }catch(PriceAccessException ex){
            log.error(ErrorConstants.PRICE_ACCESS_EXCEPTION_MESSAGE +instrumentId+". Error message :"+ex.getMessage());
            return null;
        } catch (Exception e){
            log.error("Exception occurred which accessing the latest price of instrumentId "+instrumentId+". Error message :"+e.getMessage());
            return null;
        }
        return instrumentPrice;

    }
}
