package org.neo4j.index.chronicle;

import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.util.Map;

public class RawChronicleTest {

    protected static final int COUNT = 10000;
    protected static final int RUNS = 100;
    private Map<Object,long[]> map;

    @Before
    public void setUp() throws Exception {
        File directory = new File("target/chronicle");
        FileUtils.deleteRecursively(directory);
        directory.mkdirs();
        File file = new File(directory, "chronicle-index-tree.db" + String.valueOf(1));
        file.createNewFile();
        ChronicleMapBuilder<Object, long[]> builder = ChronicleMapBuilder.of(Object.class, long[].class);
        map = builder.createPersistedTo(file);
    }

    @After
    public void tearDown() throws Exception {
        map.clear();
    }

    @Test
    public void testInsertPerformanceWithIntValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() { public Object from(int value) { return value; } });
    }

    @Test
    public void testInsertPerformanceWithLongValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() { public Object from(int value) { return (long) value; } });
    }

    @Test
    public void testInsertPerformanceWithStringValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() { public Object from(int value) { return String.valueOf(value); } });
    }

    public void insertManyNodesWithIndex(PropertyValue propertyValue) throws Exception {
        long time = System.currentTimeMillis();
        for (int run = 0; run < RUNS; run++) {
            for (int i = 0; i < COUNT; i++) {
                map.put(propertyValue.from(i), new long[]{i});
            }

        }
        time = System.currentTimeMillis() - time;
        final String type = propertyValue.from(0).getClass().getSimpleName();
        System.out.println("Creating " + COUNT * RUNS + " nodes with " + getClass().getSimpleName() + " for " + type + " properties took " + time + " ms.");
    }

}
