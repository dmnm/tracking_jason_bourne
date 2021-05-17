package org.example.operations;

import lombok.val;
import org.example.dto.Source;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class InputFileOps {
    public Flux<String> readLines(Source source) {
        return fromPath(Path.of(source.getFile()));
    }

    private Flux<String> fromPath(Path path) {
        return Flux.using(
            () -> Files.lines(path),
            stream -> Flux.fromStream(stream)
                          .publishOn(Schedulers.boundedElastic()),
            Stream::close
        );
    }

    // TODO:: think how to optimize file name parsing
    public Source toSource(String pathToFile) {
        val fileName = Path.of(pathToFile).getFileName().toString();
        val name = fileName.substring(0, fileName.lastIndexOf('.'));
        val split = name.split("_");

        //TODO:: hope that, for instance "any_post_<id>.csv" doesn't contain "_" in <id>
        val id = split[split.length - 1];
        val source = name.substring(0, name.lastIndexOf('_'));
        val type = extractSourceIdType(fileName);

        return new Source(id, source, type, pathToFile);
    }

    private Source.Type extractSourceIdType(String fileName) {
        val split = fileName.split("\\.");
        val extension = split[split.length - 1];
        return Source.Type.valueOf(extension.toLowerCase());
    }
}
