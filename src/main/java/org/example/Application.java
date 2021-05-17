package org.example;

import lombok.val;
import org.example.dto.Record;
import org.example.dto.Source;
import org.example.dto.TrackingResult;
import org.example.operations.InputFileOps;
import org.example.operations.RecordOps;
import org.example.operations.TrackingOps;
import org.example.service.LuceneIndex;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.Arrays;
import java.util.function.Consumer;

import static reactor.core.scheduler.Schedulers.boundedElastic;

public class Application implements Closeable {
    private final String trackingObject = "Jason Bourne"; // shouldn't it be a parameter or a configuration?

    private final TrackingOps trackingOps = new TrackingOps();
    private final InputFileOps fileOps = new InputFileOps();
    private final RecordOps recordOps = new RecordOps();
    private final LuceneIndex luceneIndex = LuceneIndex.instance();

    public static void main(String[] args) {
        try (val application = new Application()) {
            application.run(args);
        }
    }

    private void run(String[] args) {
        runDataIngestion(args);
        runTracking(
            trackingObject,
            flux -> flux.doOnNext(System.out::println).blockLast()
        );
    }

    //open for testing purposes only
    void runTracking(String trackingObject, Consumer<Flux<TrackingResult>> callback) {
        var flux = trackingOps.runTracking(trackingObject, luceneIndex::fuzzySearch);
        callback.accept(flux);
    }

    //open for testing purposes only
    void runDataIngestion(String[] files) {
        Flux.just(files)
            .map(fileOps::toSource)
            .flatMap(this::toRecords)
            .subscribeOn(boundedElastic())
            .flatMap(this::indexing)
            .doOnComplete(luceneIndex::commit)
            .blockLast();
    }

    private Flux<Record> toRecords(Source source) {
        val lines = fileOps.readLines(source);
        return recordOps.getRecords(source, lines);
    }

    private <T> Mono<T> indexing(Record record) {
        return Mono.<T>fromRunnable(() -> luceneIndex.index(record))
                   .publishOn(boundedElastic());
    }

    @Override
    public void close() {
        luceneIndex.close();
    }

    public static void trace(Object... args) {
        val sb = new StringBuilder()
            .append(System.currentTimeMillis()).delete(0, 9)
            .append('\t')
            .append(Thread.currentThread().getName());

        Arrays.stream(args)
              .peek(ignore -> sb.append('\t'))
              .forEach(sb::append);

        System.out.println(sb);
    }
}
