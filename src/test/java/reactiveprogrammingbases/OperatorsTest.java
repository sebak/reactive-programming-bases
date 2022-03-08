package reactiveprogrammingbases;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OperatorsTest {

    /**
     * reactor work only in main thread unless we apply concurrency
     * Let say that now we want to subscribe in amount of thread (util when we handle I/O, external API)
     * Schedulers.single() only one Thread.
     * When we do subscribeOn we are not subscribing to the publisher it will not trigger the data
     * we can create our own Scheduler or use those are provided by reactor (read doc)
     */
    @Test
    public void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(integer -> {
                    log.info("Map 1 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                }) // a chaque fois qu'on utilise map on retourne un objet immutable
                .subscribeOn(Schedulers.single()) // subscribeOn will be applied on entire flux that why we can put it anywhere (here it is on middle between two map)
                .map(integer -> {
                    log.info("Map 2 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux) // create will subscribe
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    /**
     * when you execute you will see that the first map is execute on main thread and the second map after publishOn in another thread
     * the publishOn not affect all the chain like subscribeOn but only what is after
     */
    @Test
    public void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .map(integer -> {
                    log.info("Map 1 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                }) // a chaque fois qu'on utilise map on retourne un objet immutable
                .publishOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("Map 2 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux) // create will subscribe
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }


    @Test
    public void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single()) // even if we have another type of Scheduler only the first one will be taken in account
                .map(integer -> {
                    log.info("Map 1 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                }) // a chaque fois qu'on utilise map on retourne un objet immutable
                .subscribeOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("Map 2 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux) // create will subscribe
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    /**
     * The first .publishOn Schedulers will be use on the map just after and another Scheduler will be use on second map
     */
    @Test
    public void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(integer -> {
                    log.info("Map 1 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                }) // a chaque fois qu'on utilise map on retourne un objet immutable
                .publishOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("Map 2 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux) // create will subscribe
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * here publishOn will take over on subscribeOn so the Scheduler single will be use and not the boundedElastic
     */
    @Test
    public void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(integer -> {
                    log.info("Map 1 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                }) // a chaque fois qu'on utilise map on retourne un objet immutable
                .subscribeOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("Map 2 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux) // create will subscribe
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * normally subscribeOn affect all after it but  when there is publishOn it will just use subscribeOn(Schedulers.single()) for the first map
     * and the second map will use publishOn(Schedulers.boundedElastic())
     */
    @Test
    public void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(integer -> {
                    log.info("Map 1 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                }) // a chaque fois qu'on utilise map on retourne un objet immutable
                .publishOn(Schedulers.boundedElastic())
                .map(integer -> {
                    log.info("Map 2 - Numner {} on Thread {}", integer, Thread.currentThread().getName());
                    return integer;
                });

        StepVerifier.create(flux) // create will subscribe
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    /**
     * when we want to have thread working in background( file (I/O), db activity or a long one), we should use fromCallable
     */
    @Test
    public void subscribeOnIO() throws Exception {
        Mono<List<String>> listMono = Mono.fromCallable(() -> Files.readAllLines(Paths.get("text-file.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        listMono.subscribe(s -> log.info("{} ", s)); // it is handled on background thread

        Thread.sleep(2000); // to see what happen we sleep a main thread to receive results from background Thread

        StepVerifier.create(listMono)
                .expectSubscription()// we expect subscription
                .thenConsumeWhile(list -> {
                    Assertions.assertFalse(list.isEmpty());
                    log.info("Size {}", list.size());
                    return true;
                })
                .verifyComplete();
    }

    // if the publisher return a empty result if is like if else
    @Test
    public void switchIfEmptyOperator() throws Exception {
        Flux<Object> flux = emptyFlux()
                .switchIfEmpty(Flux.just("not empty anymore"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()// we expect subscription
                .expectNext("not empty anymore")
                .expectComplete()
                .verify();
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    // it delay execution of what we have in defer
    @Test
    public void deferOperator() throws Exception {
        Mono<Long> mono = Mono.just(System.currentTimeMillis());

        // even if we sleep between displaying all value will be the same because Mono.just is capturing the value at the instantiation so we need to use defer
        mono.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        mono.subscribe(l -> log.info("time {}", l));

        log.info("\n**************** Defer ********************\n");
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100);
        defer.subscribe(l -> log.info("time {}", l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);

    }

    /**
     * merge two fux by waiting that the first flux finish emitting before the second one start to do so
     */
    @Test
    public void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(flux1, flux2).log();

        StepVerifier
                .create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();

    }

    @Test
    public void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = flux1.concatWith(flux2).log();

        StepVerifier
                .create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();

    }

    @Test
    public void combineLastOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combineLast = Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase()).log();

        StepVerifier
                .create(combineLast)
                .expectSubscription()
                .expectNext("BC", "BD") // this combination is always guarantee, it depend on how fast the flux emit
                .expectComplete()
                .verify();
    }

    /**
     * here the merge is not waiting that the first finish emitting before we merge
     * to see it let delay the first flux, the second one will emit without waiting the first one
     */
    @Test
    public void mergeOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.merge(flux1, flux2).log(); // mergeWith is the same thing as merge

        mergeFlux.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier
                .create(mergeFlux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .expectComplete()
                .verify();
    }

    /**
     * run on different thread but display sequentially data
     */
    @Test
    public void mergeSequentialOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.mergeSequential(flux1, flux2, flux1).log(); // e merge flux1 with flux2 and we merge flux1 again

        mergeFlux.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier
                .create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .expectComplete()
                .verify();
    }

    /**
     * we will expect a and after an error
     */
    @Test
    public void concatOperatorError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(flux1, flux2).log();

        StepVerifier
                .create(concatFlux)
                .expectSubscription()
                .expectNext("a")
                .expectError()
                .verify();

    }

    /**
     * we will expect a, c, d and delay the error that will happen after all flux that those not raise error are published.
     * It is use in case we want to continue even when we have error
     */
    @Test
    public void concatOperatorDelayError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concatDelayError(flux1, flux2).log();

        StepVerifier
                .create(concatFlux)
                .expectSubscription()
                .expectNext("a", "c", "d")
                .expectError()
                .verify();

    }

    /**
     * we will expect a, c, d and delay the error that will happen after all flux that those not raise error are published.
     * It is use in case we want to continue even when we have error
     */
    @Test
    public void mergeDelayError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                }).doOnError(f -> log.error("We could do something wit a failure value %s", f.getMessage()));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = Flux.mergeDelayError(1, flux1, flux2).log(); // understand the first parameter

        StepVerifier
                .create(merge)
                .expectSubscription()
                .expectNext("a", "c", "d")
                .expectError()
                .verify();

    }


    /**
     * we must know that order in flatMap is not always preserve
     */
    @Test
    public void flatMapOperator() {
        Flux<String> flux = Flux.just("a", "b");
        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::findByName)
                .log();

        flatFlux.subscribe(o -> log.info(toString()));

        StepVerifier
                .create(flatFlux)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();
    }

    public Flux<String> findByName(String name) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2") : Flux.just("nameB1", "nameB2");
    }

    /**
     * we must know that order in flatMap is not always preserve let see it by delaying the publisher
     */
    @Test
    public void flatMapOperatorWithPublisherDelay() throws InterruptedException {
        Flux<String> flux = Flux.just("a", "b");
        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::findByNameDelay)
                .log();

        flatFlux.subscribe(log::info);

        Thread.sleep(500);

        StepVerifier
                .create(flatFlux)
                .expectSubscription()
                .expectNext("nameB1", "nameB2", "nameA1", "nameA2")
                .verifyComplete();
    }

    public Flux<String> findByNameDelay(String name) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(100)) : Flux.just("nameB1", "nameB2");
    }

    /**
     * we must know that order in flatMap is not always preserve let see it by delaying the publisher as in previous test
     * but now let say we want to keep it sequential instead of delay
     */
    @Test
    public void flatMapSequentialOperator() throws InterruptedException {
        Flux<String> flux = Flux.just("a", "b");
        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByNameDelay)
                .log();

        flatFlux.subscribe(log::info);

        Thread.sleep(500);

        StepVerifier
                .create(flatFlux)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();
    }
}
