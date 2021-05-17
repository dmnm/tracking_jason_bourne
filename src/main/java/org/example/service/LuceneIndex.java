package org.example.service;

import com.google.gson.Gson;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.example.dto.Record;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

@SuppressWarnings("deprecation")
public class LuceneIndex implements Closeable {
    private static final Gson gson = new Gson();
    private static final String __src__ = "__src__";

    private final Analyzer analyzer;
    private final Directory index;
    private final IndexWriter writer;

    private volatile IndexSearcher searcher;

    private volatile static LuceneIndex instance = new LuceneIndex();

    public static LuceneIndex instance() {
        return instance;
    }

    @SneakyThrows
    private LuceneIndex() {
        analyzer = new StandardAnalyzer();
        CustomAnalyzer.builder()
                      .withTokenizer("keyword")
                      .addTokenFilter("asciifolding")
                      .addTokenFilter("lowercase")
                      .addTokenFilter("patternreplace", "pattern", "[^a-z0-9]", "replacement", "");

        index = new MMapDirectory(Path.of(".idx", String.valueOf(System.currentTimeMillis()))) /* RAMDirectory() */;
        writer = new IndexWriter(index, new IndexWriterConfig(analyzer));
        writer.commit();

        searcher = new IndexSearcher(DirectoryReader.open(index));
    }

    @SneakyThrows
    public void index(Record record) {
        var document = new Document();

        for (val identifier : record.getIdentifiers()) {
            val field = identifier.getType().name();
            val value = identifier.getValue();

            document.add(new StringField(
                field,
                normalize(value),
                Field.Store.NO
            ));
        }

        document.add(new StoredField(__src__, gson.toJson(record)));

        writer.addDocument(document);
    }

    // for testing purposes only
    @SneakyThrows
    void index(String field, String text) {
        val document = new Document();
        document.add(new StringField(field, normalize(text), Field.Store.NO));
        writer.addDocument(document);
    }


    /**
     * Implements the fuzzy search.
     * The similarity measurement is based on the Damerau-Levenshtein (optimal string alignment) algorithm.
     * At most, this search will match records up to 2 edits.
     * Higher distances (especially with transpositions enabled), are generally not useful.
     */
    @SneakyThrows
    public List<Record> fuzzySearch(String field, String text) {
        val term = new Term(field, normalize(text));
        val query = new FuzzyQuery(term);

        val topDocs = searcher.search(query, Integer.MAX_VALUE);
        var records = new ArrayList<Record>();
        for (val scoreDoc : topDocs.scoreDocs) {
            val doc = searcher.doc(scoreDoc.doc);
            val src = doc.get(__src__);
            val record = gson.fromJson(src, Record.class);
            records.add(record);
        }

        return unmodifiableList(records);
    }

    @SneakyThrows
    public synchronized void commit() {
        writer.commit();
        searcher = new IndexSearcher(DirectoryReader.open(index));
    }


    @Override
    @SneakyThrows
    public synchronized void close() {
        searcher.getIndexReader().close();
        writer.close();
        analyzer.close();
        index.close();
        LuceneIndex.instance = new LuceneIndex();
    }

    private String normalize(String string) {
        return string.toLowerCase().replaceAll("[^a-z0-9]", "");
    }
}
