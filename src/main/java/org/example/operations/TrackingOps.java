package org.example.operations;

import lombok.val;
import org.example.dto.IdentityType;
import org.example.dto.Record;
import org.example.dto.TrackingResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

public class TrackingOps {
    public Flux<TrackingResult> runTracking(String trackingObject, BiFunction<String, String, List<Record>> findNext) {
        val tracking = new Tracking(findNext).submit(IdentityType.name, trackingObject);
        return tracking.asFlux();
    }

    private static final class Tracking {
        final Sinks.Many<Tuple2<IdentityType, String>> sink = Sinks.many().multicast().onBackpressureBuffer();
        final Set<TrackingResult> visited = ConcurrentHashMap.newKeySet();
        final BiFunction<String, String, List<Record>> findNext;
        final Flux<TrackingResult> flux;

        Tracking(BiFunction<String, String, List<Record>> findNext) {
            this.findNext = findNext;
            this.flux = toFlux(sink);
        }

        TrackingResult toTrackingResult(Record record) {
            return new TrackingResult(record.getSource().getFile(), record.getDateTime(), record.getFullName());
        }

        Flux<TrackingResult> toFlux(Sinks.Many<Tuple2<IdentityType, String>> sink) {
            return sink.asFlux()
                       .flatMapIterable(t -> findNext.apply(t.getT1().name(), t.getT2()))
                       .publishOn(Schedulers.boundedElastic())
                       .map(record -> Tuples.of(toTrackingResult(record), record))
                       .filter(it -> visited.add(it.getT1()))
                       .doOnNext(this::submitAll)
                       .map(Tuple2::getT1)
                       .timeout(Duration.of(3, ChronoUnit.SECONDS))
                       .onErrorResume(TimeoutException.class, this::cancel) // workaround to 'complete' the sink
                ;
        }

        void submitAll(Tuple2<TrackingResult, Record> tuple) {
            tuple.getT2().getIdentifiers().forEach(identifier -> submit(identifier.getType(), identifier.getValue()));
        }

        Tracking submit(IdentityType type, String value) {
            sink.tryEmitNext(Tuples.of(type, value));
            return this;
        }

        <T> Mono<T> cancel(TimeoutException ignore) {
            System.out.println("Finished!");
            sink.tryEmitComplete();
            return Mono.empty();
        }

        Flux<TrackingResult> asFlux() {
            return flux;
        }
    }
}
