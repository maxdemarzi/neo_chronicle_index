package org.neo4j.index.chronicle.provider;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.*;
import org.neo4j.register.Register;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class ChronicleIndex extends IndexAccessor.Adapter implements IndexPopulator, IndexUpdater {

    private final Map<Object, long[]> indexData;

    private InternalIndexState state = InternalIndexState.POPULATING;

    public ChronicleIndex(final Map<Object, long[]> map){
        this.indexData = map;
    }

    @Override
    public void create() throws IOException {
        this.indexData.clear();
    }

    public InternalIndexState getState() {
        return this.state;
    }

    @Override
    public void add(final long nodeId, final Object propertyValue) throws IndexEntryConflictException, IOException, IndexCapacityExceededException {
        long[] nodes = this.indexData.get(propertyValue);
        if (nodes==null || nodes.length==0) {
            this.indexData.put(propertyValue, new long[]{nodeId});
            return;
        }
        final int idx=this.indexOf(nodes,nodeId);
        if (idx!=-1) return;
        nodes = Arrays.copyOfRange(nodes, 0, nodes.length + 1);
        nodes[nodes.length-1]=nodeId;
        this.indexData.put(propertyValue, nodes);
    }

    @Override
    public void verifyDeferredConstraints(PropertyAccessor propertyAccessor) throws Exception {

    }

    @Override
    public IndexUpdater newPopulatingUpdater(PropertyAccessor propertyAccessor) throws IOException {
        return this;
    }

    @Override
    public void close(boolean populationCompletedSuccessfully) throws IOException, IndexCapacityExceededException {
        if (populationCompletedSuccessfully) {
            this.state = InternalIndexState.ONLINE;
        }
    }

    @Override
    public void markAsFailed(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sampleResult(Register.DoubleLong.Out out) {

        // TODO

        return 0;
    }

    @Override
    public Reservation validate(Iterable<NodePropertyUpdate> iterable) throws IOException, IndexCapacityExceededException {
        return null;
    }

    @Override
    public void process(final NodePropertyUpdate update) throws IOException, IndexEntryConflictException, IndexCapacityExceededException {
        switch (update.getUpdateMode()) {
            case ADDED:
                this.add(update.getNodeId(), update.getValueAfter());
                break;
            case CHANGED:
                this.removed(update.getNodeId(), update.getValueBefore());
                this.add(update.getNodeId(), update.getValueAfter());
                break;
            case REMOVED:
                this.removed(update.getNodeId(), update.getValueBefore());
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void remove(PrimitiveLongSet nodeIds) throws IOException {
        final Iterator<Map.Entry<Object,long[]>> entries = this.indexData.entrySet().iterator();
        while (entries.hasNext()) {
            final Map.Entry<Object, long[]> entry = entries.next();
            long[] nodes = entry.getValue();
            int existingCount = nodes.length;

            final PrimitiveLongIterator nodeIdIter = nodeIds.iterator();

            while(nodeIdIter.hasNext()) {

                final long nodeId = nodeIdIter.next();

                final int idx = this.indexOf(nodes, nodeId);
                if (idx != -1) {
                    if (existingCount == 1) {
                        entries.remove();
                        break;
                    } else {
                        System.arraycopy(nodes,idx,nodes,idx-1, existingCount-idx-1);
                        nodes = Arrays.copyOfRange(nodes, 0, existingCount - 1);
                        entry.setValue(nodes);
                        existingCount--;
                    }
                }
            }
        }
    }

    private void removed(final long nodeId, final Object propertyValue) {
        long[] nodes = this.indexData.get(propertyValue);
        if (nodes==null || nodes.length ==0) return;
        final int idx=this.indexOf(nodes,nodeId);
        if (idx==-1) return;
        final int existingCount = nodes.length;
        if (existingCount == 1) {
            this.indexData.remove(propertyValue);
            return;
        }
        System.arraycopy(nodes,idx,nodes,idx-1, existingCount-idx-1);
        nodes = Arrays.copyOfRange(nodes, 0, existingCount - 1);
        this.indexData.put(propertyValue, nodes);
    }

    private int indexOf(final long[] nodes, final long nodeId) {
        for (int i = nodes.length - 1; i != 0; i--) {
            if (nodes[i]==nodeId) return i;
        }
        return -1;
    }

    @Override
    public IndexReader newReader() {
        return new ChronicleIndexReader((Map<Object, long[]>) this.indexData);
    }
}
