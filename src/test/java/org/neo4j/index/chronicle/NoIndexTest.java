package org.neo4j.index.chronicle;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Label;

public class NoIndexTest extends BasicIndexTest {
    @Override
    protected void createIndex(Label l) {
    }

    @Test
    @Ignore
    public void testCreateAddIndex() throws Exception {

    }
}
