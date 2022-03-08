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
import java.util.List;

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

    // it delay execution of what we have in defer
    @Test
    public void deferOperator() throws Exception {
        Flux<Object> flux = emptyFlux()
                .switchIfEmpty(Flux.just("not empty anymore"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()// we expect subscription
                .expectNext("not empty anymore")
                .expectComplete()
                .verify();
    }

    private Flux<Object> emptyFlux(){
        return Flux.empty();
    }
}
