package org.neo4j.index.chronicle;

import org.junit.Before;

import java.io.IOException;

public class LuceneIndexTest extends BasicIndexTest {
    @Override
    @Before
    public void setUp() throws IOException {
        ChronicleSchemaIndexProvider.PRIORITY = 0;
        super.setUp();
    }
}

