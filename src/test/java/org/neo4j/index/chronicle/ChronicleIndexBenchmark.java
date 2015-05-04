package org.neo4j.index.chronicle;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class ChronicleIndexBenchmark {

    protected GraphDatabaseService db;
    protected String PROPERTY = "bar";
    protected IndexDefinition indexDefinition;

    @Param({"100000"})
    public int COUNT;

    @Setup
    public void prepare() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        createIndex(DynamicLabel.label("fooint"));
        createIndex(DynamicLabel.label("foolong"));
        createIndex(DynamicLabel.label("foostring"));
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
    @Fork(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testChronicleInsertPerformanceWithIntValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return value;
            }
        }, "int");
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
    @Fork(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testChronicleInsertPerformanceWithLongValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return (long) value;
            }
        }, "long");
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
    @Fork(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testChronicleInsertPerformanceWithStringValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return String.valueOf(value);
            }
        }, "string");
    }

    protected void createIndex(Label label) {
        try (Transaction tx = db.beginTx()) {
            final IndexCreator indexCreator = db.schema().indexFor(label).on(PROPERTY);
            indexDefinition = indexCreator.create();
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            db.schema().awaitIndexOnline(indexDefinition, 1, TimeUnit.DAYS);
            tx.success();
        }
    }

    public void insertManyNodesWithIndex(PropertyValue propertyValue, String ext) throws Exception {
        final Label label = DynamicLabel.label("foo"+ ext);
        final Label[] labels = new Label[]{label};
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < COUNT; i++) {
                final Node node = db.createNode(labels);
                node.setProperty(PROPERTY, propertyValue.from(i));
            }
            tx.success();
        }
    }


}
