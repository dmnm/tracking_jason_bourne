### Pinpoint Jason Bourne's position

####

#### Requirements:

- Java 11
- Maven 3

####

#### How to build:

```shell
$ cd tracking_jason_bourne
$ mvn clean package
```

####

#### How to run:

```shell
$ java -jar target/tracking_jason_bourne-1.0-SNAPSHOT-jar-with-dependencies.jar \
post_n1.csv \
post_n2.csv \
post_n3.csv
```

Output:

```shell
post_n1.csv: 2021-05-15T07:00:00.0000Z, Jason Bourne
post_n3.csv: 2021-05-15T06:15:15.0000Z, David Webb
post_n2.csv: 2021-05-15T12:13:44.0000Z, Delta One
```

###

#### Features:
- CSV input file format
- memory agnostic algorithm (files may not fit in memory)
- reactive programming paradigm
- parallel and concurrent input data utilization (with declarative concurrency)
- work with misnomers, misspellings
- packaged as a single jar

#### Not implemented yet:
- JSON input file format
- complex identifiers like photos, fingerprints, face ids, etc
