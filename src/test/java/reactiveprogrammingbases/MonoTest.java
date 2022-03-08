package reactiveprogrammingbases;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
/**
 * Reactive Streams have to be
 * 1- asynchronous
 * 2- non-blocking
 * 3- backpressure
 * -------- 4 Interfaces ---------------
 * - Publisher: the one that create event it is cold (to activate him we must subscribe).
 *  it have one method subscribe(...) where the Subscriber call to start to send him data:
 *  1- When the Subscriber subscribe by calling the subscribe method, then a Object  witch is a kind of context that is created (Subscription)
 *  2- Publisher will call Subscriber method onSubscribe(...) with Subscription Object to tell him that i am aware of your subscription: now the Subscription
 *  can be used to handle the backPressure (see Subscription object who have two method request(long var) use by the Subscriber to tell a publisher just send
 *  me var number of data because i can't handle more. also cancel() method
 *  3- the Publisher call onNext method from Subscriber to send data to Subscriber. The onNext will be call until one of those 3 conditions are meet:
 *      - The Publisher sent all the requested object. (that mean that if the Publisher can produce 10 cakes and the Subscriber by Subscription object tell him just send me
 *      5 cakes (by using request(...) method) when the 5 cakes are sent the Subscription Object is cancel or Subscriber can ask more data or the rest of cake by recalling request method.
 *      - When the Publisher have sent all his data, so it will call from Subscriber the onComplete method to tell i have finish my job, then Subscriber and Subscription object
 *      will be cancelled.
 *      - if something bad happen, error exception etc... so Publisher will call onError from Subscriber and Subscriber and Subscription object will be cancelled
 *
 *
 */
public class MonoTest {
    @Test
    public void monoSubscriber() {
        String name = "Picol Wasnyo";
        // this Mono is a producer with cold code. if we do log.info("Mono {}", mono) we wi never see a mono content since no one subscribe
        Mono<String> mono = Mono.just(name)
                .log(); // allow us to see what happening (in terminal, the different method call, subscribe, onNext etc..) inside the publisher when we subscribe

        mono.subscribe();// dans subscriber on pourrait récupérer le string et faire des transformation

        log.info("---------------------------------------------------------------------------");

        /**
         * partie test(le code en haut c'est ce qui pourrait être dans une methode qu'on testerait comme ci-dessous)
         * le test fait directement un subscribe via StepVerifier
         */
        StepVerifier.create(mono)
                .expectNext(name)   // quand tu publies tu expect en OnNext le name
                .verifyComplete(); // verifie qu'on a onComplete ce qui va aussi faire la subscription au Mono créé dans le create

    }

    @Test
    public void monoSubscriberConsumer() {
        String name = "Picol Wasnyo";
        Mono<String> mono = Mono.just(name)
                .log(); // allow us to see what happening (in terminal, the different method call, subscribe, onNext etc..) inside the publisher when we subscribe

        mono.subscribe(s -> log.info("Value name is: {}", s));// subscriber have different overload method with different parameters (let use one with Consumer)

        log.info("---------------------------------------------------------------------------");

        /**
         * partie test(le code en haut c'est ce qui pourrait être dans une methode qu'on testerait comme ci-dessous)
         * le test fait directement un subscribe via StepVerifier
         */
        StepVerifier.create(mono)
                .expectNext(name)   // quand tu publies tu expect en OnNext le name
                .verifyComplete(); // verifie qu'on a onComplete ce qui va aussi faire la subscription au Mono créé dans le create

    }

    @Test
    public void monoSubscriberError() {
        String name = "Picol Wasnyo";
        Mono<String> mono = Mono.just(name)
                .map(s -> {
                    throw new RuntimeException("Testing Mono With error");
                }); // we can't have log() after this

        mono.subscribe(s -> log.info("Value name is: {}", s), s -> log.error("Something create and error"));// with 2 consumer

        log.info("----------------------------------With stack trace to see error (it is a good pratice-----------------------------------------");
        mono.subscribe(s -> log.info("Value name is: {}", s), Throwable::printStackTrace);

        log.info("---------------------------------------------------------------------------");

        /**
         * partie test(le code en haut c'est ce qui pourrait être dans une methode qu'on testerait comme ci-dessous)
         * le test fait directement un subscribe via StepVerifier
         */
        StepVerifier.create(mono)
                .expectError(RuntimeException.class)   // now we expect error on RuntimeException type
                .verify(); // when we have and error we can't have onComplete so we remove verifyComplete() to just have verify()

    }

    @Test
    public void monoSubscriberConsumerComplete() {
        String name = "Picol Wasnyo";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value name is: {}", s), Throwable::printStackTrace, () -> log.info("FINISHED")); // with runnable as third parameter

        // NB: when we execute, in terminal if we see request(unbounded), that mean that we have not precise the number of data to send by producer, so it will send all his data

        log.info("---------------------------------------------------------------------------");

        /**
         * partie test(le code en haut c'est ce qui pourrait être dans une methode qu'on testerait comme ci-dessous)
         * le test fait directement un subscribe via StepVerifier
         */
        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();

    }

    @Test
    public void monoSubscriberConsumerCompleteSubscription() {
        String name = "Picol Wasnyo";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        /** with a four parameter that is a kind of relation between Subscriber and Publisher so something (method) from Subscription
         * here we call cancel to tell the publisher to stop produce after sending data by deleting Subscriber and Subscription to clean
         * since this is a Mono it just sent one data and stop, so it has not a lot of sense here to do Subscription::cancel here because it is automatically done after
         * Mono sent his only data
         */
        mono.subscribe(s -> log.info("Value name is: {}", s), Throwable::printStackTrace, () -> log.info("FINISHED"), Subscription::cancel);

        log.info("---------------------------------------------------------------------------");


        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();

    }

    @Test
    public void monoSubscriberConsumerAskNDataAndCompleteSubscription() {
        String name = "Picol Wasnyo";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        /** with a four parameter that is a kind of relation between Subscriber and Publisher so something (method) from Subscription
         * here we call request(N) to tell the publisher to produce N time, since Mono have only one data, it will produce just one time by only one onNext calling
         * if it was a flux it will send his first N data
         */
        mono.subscribe(s -> log.info("Value name is: {}", s), Throwable::printStackTrace, () -> log.info("FINISHED"), subscription -> subscription.request(5));

        log.info("---------------------------------------------------------------------------");


        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();

    }

    @Test
    public void monoDoOnMethods() {
        String name = "Picol Wasnyo";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Some one Subscribe to us")) // trigger when subscription is done
                .doOnRequest(value -> log.info("Request received to send all data or a part of it")) // when request is as through the function request from subscriber by Subscription (value is a number of request to do)
                .doOnNext(s -> log.info("As producer i emit this here {}", s)) // call each time the producer emit, so the producer call onNext method of Subscriber to let him know
                .doOnSuccess(s -> log.info("doOnSuccess executed well because i have completed"));// doOnNext not execute when there is no thing to emit but doOnSucess is executed when it is complete successfully even do there is no data to emmit


        mono.subscribe(s -> log.info("Value I have Receive as subscriber is: {}", s), Throwable::printStackTrace, () -> log.info("FINISHED"));

    }

    @Test
    public void monoDoOnMethodsCaseWhenSecondDoOnNextNotExecuted() {
        String name = "Picol Wasnyo";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Some one Subscribe to us")) // trigger when subscription is done
                .doOnRequest(value -> log.info("Request received to send all data or a part of it")) // when request is as through the function request from subscriber by Subscription (value is a number of request to do)
                .doOnNext(s -> log.info("As producer i emit this here {}", s)) // call each time the producer emit, so the producer call onNext method of Subscriber to let him know
                .flatMap(s -> Mono.empty()) //there is nothing to emit so that tne doOnNext below will be not executed
                .doOnNext(s -> log.info("As producer i emit this here {}", s)) // not executed because nothing to emit no data
                .doOnSuccess(s -> log.info("doOnSuccess executed well because i have completed"));// doOnNext not execute when there is no thing to emit but doOnSucess is executed when it is complete successfully even do there is no data to emmit


        mono.subscribe(s -> log.info("Value I have Receive as subscriber is: {}", s), Throwable::printStackTrace, () -> log.info("FINISHED"));

    }

    @Test
    public void monoDoOnError() {

        Mono<Object> monoError = Mono.error(new IllegalArgumentException("Our IllegalArgument Exception"))
                .doOnError(throwable -> log.info("error is {}", throwable.getMessage())) // if the error happen after subscription we do this it will print: error is Our IllegalArgument Exception
                .log(); // to display the process of subscription between producer and subscriber


        // this will subscribe in our Mono and expect some stuff to test
        StepVerifier.create(monoError)
                .expectError(IllegalArgumentException.class)
                .verify();

    }

    @Test
    // to continue even if we have an error because doOnNext do nothing after error or when there is nothing to emit as empty Mono
    public void monoDoOnErrorResume() {
        String name = "Picol Wasnyo";
        Mono<Object> monoError = Mono.error(new IllegalArgumentException("Our IllegalArgument Exception"))
                .doOnError(throwable -> log.info("error is {}", throwable.getMessage()))
                .onErrorResume(s -> {
                    log.info("Inside Error Resume");
                    return Mono.just(name);
                })
                .log(); // to display the process of subscription between producer and subscriber


        // this will subscribe in our Mono and expect some stuff to test
        StepVerifier.create(monoError)
                .expectNext(name) // even do we have error it will emit after and complete because onErrorResume
                .verifyComplete();

    }

    @Test
    // to continue even if we have an error and return a simple value as a string when onErrorResume return a Mono
    public void monoDoOnErrorReturn() {
        String name = "Picol Wasnyo";
        Mono<Object> monoError = Mono.error(new IllegalArgumentException("Our IllegalArgument Exception"))
                .doOnError(throwable -> log.info("error is {}", throwable.getMessage()))
                .onErrorReturn("EMPTY")
                .log(); // to display the process of subscription between producer and subscriber


        // this will subscribe in our Mono and expect some stuff to test
        StepVerifier.create(monoError)
                .expectNext("EMPTY") // even do we have error it will emit after and complete because onErrorResume
                .verifyComplete();

    }
}
