package org.neo4j.index.chronicle.provider;

import net.openhft.chronicle.map.ChronicleMap;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.register.Register;

import java.util.Map;
import java.util.Set;

public class ChronicleIndexReader implements IndexReader {

    private static final long[] EMPTY_LONGS = new long[0];
    private ChronicleMap<Object, long[]> snapshot;
    private final NonUniqueIndexSampler nonUniqueIndexSampler;
    private final Set<Class> valueTypesInIndex;

    ChronicleIndexReader(final ChronicleMap<Object, long[]> snapshot, NonUniqueIndexSampler nonUniqueIndexSampler, Set<Class> valueTypesInIndex) {
        this.snapshot = snapshot;
        this.nonUniqueIndexSampler = nonUniqueIndexSampler;
        this.valueTypesInIndex = valueTypesInIndex;
    }


    @Override
    public PrimitiveLongIterator lookup(Object value) {
        final long[] result = snapshot.get(value);
        return PrimitiveLongCollections.iterator(result == null || result.length == 0 ? EMPTY_LONGS : result);
    }

    @Override
    public int getIndexedCount(final long nodeId, final Object propertyValue) {
        final long[] result = snapshot.get(propertyValue);
        return result == null ? 0 : result.length;
    }

    @Override
    public Set<Class> valueTypesInIndex() {
        return valueTypesInIndex;
    }

    @Override
    public long sampleIndex(Register.DoubleLong.Out out) throws IndexNotFoundKernelException {
        return nonUniqueIndexSampler.result(out);
    }

    @Override
    public void close() {}
}
