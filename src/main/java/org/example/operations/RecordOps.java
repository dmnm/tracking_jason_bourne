package org.example.operations;

import lombok.val;
import org.example.dto.Identifier;
import org.example.dto.IdentityType;
import org.example.dto.Record;
import org.example.dto.Source;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.util.function.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RecordOps {
    public Flux<Record> getRecords(Source source, Flux<String> lines) {
        switch (source.getType()) {
            case json:
                return jsonRecords(source, lines);
            case csv:
                return csvRecords(source, lines);
            default:
                throw new IllegalStateException(source.getType() + " is not supported!");
        }

    }

    public Flux<Record> jsonRecords(Source source, Flux<String> lines) {
        throw new UnsupportedOperationException("JSON format is not supported yet!");
    }

    public Flux<Record> csvRecords(Source source, Flux<String> lines) {
        return zipWithHeaders(lines).flatMap(
            item -> toRecord(source, item.getT1(), item.getT2())
        );
    }

    private Mono<Record> toRecord(Source source, List<IdentityType> identityTypes, String line) {
        return Mono.fromSupplier(() -> {
            // looks like the first mutable and modifiable variable, good news - it's a local one
            var fields = new LinkedList<String>();
            Arrays.stream(line.split(","))
                  .map(String::trim)
                  .filter(not(String::isEmpty))
                  .forEach(fields::add);

            val dateTime = fields.pop();
            val fullName = fields.peek();
            val identities = identityTypes.stream()
                                          .map(type -> new Identifier(type, fields.pop()))
                                          .collect(toUnmodifiableList());


            return new Record(source, dateTime, fullName, identities);
        });
    }

    //open for testing purposes only
    Flux<Tuple2<List<IdentityType>, String>> zipWithHeaders(Flux<String> lines) {
        return lines.switchOnFirst(
            (firstLine, flux) -> Flux.zip(
                headersFrom(firstLine),
                flux.skip(1)
            )
        );
    }

    private Flux<List<IdentityType>> headersFrom(Signal<? extends String> firstLine) {
        if (firstLine.hasValue()) {
            val headers = parseHeader(firstLine.get());
            return Flux.just(headers).repeat();
        } else {
            return Flux.empty();
        }
    }

    private List<IdentityType> parseHeader(String headersLine) {
        return Arrays.stream(headersLine.split(","))
                     .map(String::trim)
                     .filter(not(String::isEmpty))
                     .map(String::toLowerCase)
                     .filter(not("timestamp"::equals)) // ignore timestamp column header
                     .map(this::fieldMapping)
                     .collect(toUnmodifiableList());
    }

    /*
     * TODO:: probably we have to have a field mapping for fields from different posts to an original and unique type
     * Currently to simplify logic we just use them 'as is',
     * and expect that the same identities in different sources are aligned and defined as in IdentityType enum
     */
    private IdentityType fieldMapping(String fieldName) {
        return IdentityType.valueOf(fieldName);
    }
}
