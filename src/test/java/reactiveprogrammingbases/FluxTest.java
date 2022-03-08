package reactiveprogrammingbases;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class FluxTest {

    @Test
    public void fluxSubscriber() {

        Flux<String> stringFlux = Flux.just("Peter", "James", "Olivier")
                .log();

        StepVerifier.create(stringFlux) // test create an automatic subscriber (execute and look to terminal to see called method)
                .expectNext("Peter", "James", "Olivier")
                .verifyComplete();

    }

    @Test
    public void fluxSubscriberNumber() {

        Flux<Integer> integerFlux = Flux.range(1, 5)
                .log();

        integerFlux.subscribe(integer -> log.info("Number {}", integer));

        log.info("---------------------------------------------------------");
        StepVerifier.create(integerFlux) // test create an automatic subscriber (execute and look to terminal to see called method)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

    }

    @Test
    public void fluxSubscriberFromList() {

        Flux<Integer> integerFlux = Flux.fromIterable(Arrays.asList(1, 2, 3))
                .log();

        integerFlux.subscribe(integer -> log.info("Number {}", integer));

        log.info("---------------------------------------------------------");
        StepVerifier.create(integerFlux) // test create an automatic subscriber (execute and look to terminal to see called method)
                .expectNext(1, 2, 3)
                .verifyComplete();

    }

    @Test
    public void fluxSubscriberNumberError() {

        Flux<Integer> integerFlux = Flux.range(1, 5)
                .log()
                .map(integer -> {
                    if (integer == 4) { // when error is thrown the cancel method of Subscription object is call look the terminal
                        throw new IndexOutOfBoundsException("index errorr");
                    }
                    return integer;
                });

        integerFlux.subscribe(integer -> log.info("Number {}", integer), Throwable::printStackTrace, () -> log.info("FINISHED"));

        log.info("---------------------------------------------------------");
        StepVerifier.create(integerFlux) // test create an automatic subscriber (execute and look to terminal to see called method)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();

    }

    @Test
    public void fluxSubscriberNumberUsingBadBackPressure() {

        Flux<Integer> integerFlux = Flux.range(1, 5)
                .log()
                .map(integer -> {
                    if (integer == 4) { // when error is thrown the cancel method of Subscription object is call look the terminal
                        throw new IndexOutOfBoundsException("index errorr");
                    }
                    return integer;
                });

        // The first subscriber will not throw error because we request 3 data before data 4 who will create error (look the terminal)
        integerFlux.subscribe(integer -> log.info("Number {}", integer),
                Throwable::printStackTrace, () -> log.info("DONE"),
                subscription -> subscription.request(3)); // we use the four parameter to handle backpressure by asking only 3 data, here we will never rich to error thrown when value is 4

        log.info("---------------------------------------------------------");
        StepVerifier.create(integerFlux) // test create an automatic subscriber (execute and look to terminal to see called method)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();

    }

    @Test
    public void fluxSubscriberNumberUsingBackPressure() {

        Flux<Integer> integerFlux = Flux.range(1, 10)
                .log();


        integerFlux.subscribe(new Subscriber<Integer>() {
            private int count = 0;
            private Subscription subscription;
            private final int requestCount = 2;

            // this method is call by producer when i subscribe after calling method subscribe of producer that create Subscription object to communicate with me
            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                this.subscription.request(requestCount); //we are telling producer to emit 2 data
            }

            /**
             * onNext is call by producer each time is emitted data so for producer to emit all his data not only two data as defined in onSubscribe method
             * we add here some intelligence to request all producer data by group of requestCount number, look terminal
             */
            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    this.subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });
    }

    @Test
    public void fluxSubscriberNumberUsingNotSoUglyBackPressure() {

        Flux<Integer> integerFlux = Flux.range(1, 10)
                .log();

        // we use BaseSubscriber who have already Subscription object
        integerFlux.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;

            // when we subscribe we want to do the back pressure by saying publisher by calling request of Subscription to say to only send n data
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        log.info("---------------------------------------------------------\n");
        StepVerifier.create(integerFlux) // test create an automatic subscriber (execute and look to terminal to see called method)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();

    }

    @Test
    public void fluxSubscriberNumberUsingPrettyPressure() {

        Flux<Integer> integerFlux = Flux.range(1, 10)
                //.limitRate(2) // if we put before log() request will be unbounded on terminal meaning that publisher will send all his data
                .log()
                .limitRate(2); // request will be 2 send 2 by 2 data it can replace what we have done in fluxSubscriberNumberUsingNotSoUglyBackPressure or before to make it more simple


        integerFlux.subscribe(integer -> log.info("Number {}", integer));

        log.info("---------------------------------------------------------");
        StepVerifier.create(integerFlux) // test create an automatic subscriber (execute and look to terminal to see called method)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();

    }

    // interval how we schedule after predefined time
    @Test
    public void fluxSubscriberIntervalOne() throws InterruptedException {

        // in interval of 100 millisecond it publishes a number
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .log();

        // we use BaseSubscriber who have already Subscription object
        interval.subscribe(i -> log.info("Actual {}", i));

        /* interval is executed in background because it is blocking thread so we add Thread.sleep to be able to see what is happening otherwise prompt will no display
         anything. try to execute by removing Thread.sleep to understand
         */
        Thread.sleep(3_000);

    }

    // interval how we schedule after predefined time
    @Test
    public void fluxSubscriberIntervalTake() throws InterruptedException {

        // in interval of 100 millisecond it publishes a number
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .take(10) // take the 10 first element and stop to sent data
                .log();

        // we use BaseSubscriber who have already Subscription object
        interval.subscribe(i -> log.info("Actual {}", i));

        /* interval is executed in background because it is blocking thread so we add Thread.sleep to be able to see what is happening otherwise prompt will no display
         anything. try to execute by removing Thread.sleep to understand
         */
        Thread.sleep(3_000);

    }

    // interval how we schedule after predefined time
    @Test
    public void fluxSubscriberIntervalTwo() throws InterruptedException {

        /**
         we use withVirtualTime to test because we can't way one day to test.
         The issue with this is that it will never complete the test will still wait to the next days or the nextOne, uncomment to see
         */
       /*StepVerifier.withVirtualTime(this::getIntervalEveryDay)
               .expectSubscription()// we expect subscription
               .thenAwait(Duration.ofDays(1)) // we simulate the waiting data of the first day
               .expectNext(0L) // the first element to be sent will be zero
               .verifyComplete();*/

        /**
         To correct the issue up we most cancel. let say that we are waiting for 2 day
         */
        /*StepVerifier.withVirtualTime(this::getIntervalEveryDay)
                .expectSubscription()// we expect subscription
                .thenAwait(Duration.ofDays(1)) // we simulate the waiting data of the first day
                .expectNext(0L) // the first element to be sent will be  zero the first day and one the next day
                .thenAwait(Duration.ofDays(1)) // we simulate the waiting data of the  second day
                .expectNext(1L) // the first element to be sent will be  zero the first day and one the next day
                .thenCancel() // stop to send
                .verify();*/

        /**
         here we want to verify that no data it sent before one day
         */

        StepVerifier.withVirtualTime(this::getIntervalEveryDay)
                .expectSubscription()// we expect subscription
                .expectNoEvent(Duration.ofDays(1)) // nothing is published before one day
                .thenAwait(Duration.ofDays(1)) // we simulate the waiting data of the first day
                .expectNext(0L) // the first element to be sent will be  zero the first day and one the next day
                .thenAwait(Duration.ofDays(1)) // we simulate the waiting data of the  second day
                .expectNext(1L) // the first element to be sent will be  zero the first day and one the next day
                .thenCancel() // stop to send
                .verify();

    }

    private Flux<Long> getIntervalEveryDay() {
        // every day it will print one number
        return Flux.interval(Duration.ofDays(1))
                .log();
    }

    /**
     * Connectable flux the hot one, mean that when anyone connects data are published the previous one we needed to wait that someone connect
     * before it emit
     */
    @Test
    public void connectableFlux() throws InterruptedException {

        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100)) // we publish every 100 millisecond
                .publish(); // publish create a connectable flux so at the moment we connect not subscribe the flux will start send event


        /*connectableFlux.connect();

        log.info("-------------------Thread Sleep the main thread for 300s to see what happened--------------------------------------");
        Thread.sleep(300);
        connectableFlux.subscribe(integer -> log.info("sub1 number {}", integer));

        log.info("-------------------Thread Sleep the main thread for 200s again to see what happened--------------------------------------");
        Thread.sleep(200);
        connectableFlux.subscribe(integer -> log.info("sub2 number {}", integer));*/

        StepVerifier
                .create(connectableFlux)
                .then(connectableFlux::connect) // we connect
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .expectComplete()
                .verify();

    }

    /**
     * Connectable flux the hot one, mean that when anyone connects data are published the previous one we needed to wait that someone connect
     * before it emit
     */
    @Test
    public void connectableFlux1() throws InterruptedException {

        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100)) // we publish every 100 millisecond
                .publish(); // publish create a connectable flux so at the moment we connect not subscribe the flux will start send event


        StepVerifier
                .create(connectableFlux)
                .then(connectableFlux::connect) // we connect
                .thenConsumeWhile(integer -> integer <= 5)// consume the element less or equal five
                .expectNext(6, 7, 8, 9, 10) // it will sent all the element but consume those that are <=5 and display the rest
                .expectComplete()
                .verify();

    }

    /**
     * Connectable flux the hot one, mean that when anyone connects data are published the previous one we needed to wait that someone connect
     * before it emit
     */
    @Test
    public void connectableFluxAutoConnect() throws InterruptedException {

        Flux<Integer> fluxAutoConnect = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofMillis(100)) // we publish every 100 millisecond
                .publish() // publish create a connectable flux so at the moment we connect not subscribe the flux will start send event
                .autoConnect(2); // with two subscriber


        //this will hang because is expected  a second subcriber to start to publish even because create method just create one subscriber
        /*StepVerifier
                .create(fluxAutoConnect)
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();*/

        // to avoid the hangup and have two, we have to subscribe again
        StepVerifier
                .create(fluxAutoConnect)
                .then(fluxAutoConnect::subscribe) // create the second subscriber
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();

    }

}
