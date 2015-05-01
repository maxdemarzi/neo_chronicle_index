package org.neo4j.index.chronicle;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.neo4j.index.chronicle.provider.ChronicleIndex;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.*;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.monitoring.Monitors;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.neo4j.index.chronicle.ChronicleIndexProviderFactory.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.StoreVersionMismatchHandler.ALLOW_OLD_VERSION;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class ChronicleSchemaIndexProvider extends SchemaIndexProvider {
    static int PRIORITY;
    static {
        PRIORITY = 2;
    }

    private final Map<Long, ChronicleIndex> indexes = new CopyOnWriteHashMap<>();
    private final File directory;

    public ChronicleSchemaIndexProvider(final Config config){
        super(PROVIDER_DESCRIPTOR, PRIORITY);
        this.directory = this.getDirectory(config);
    }

    private ChronicleMap<Object, long[]> getIndexFile(final long indexId)  {
        File file = new File(this.directory, "chronicle-index-tree.db" + String.valueOf(indexId));
        try {
            return ChronicleMapBuilder.of(Object.class, long[].class).createPersistedTo(file);
        } catch(IOException ioe) {
            throw new RuntimeException("Error creating chronicle index id "+indexId+"for file "+file);
        }
    }

    private File getDirectory(final Config config) {
        final File rootDirectory = this.getRootDirectory(config, PROVIDER_DESCRIPTOR.getKey());
        final File indexDirectory = new File(rootDirectory, PROVIDER_DESCRIPTOR.getVersion());
        if ((indexDirectory.exists() && indexDirectory.isDirectory()) || indexDirectory.mkdirs())
            return indexDirectory;
        throw new RuntimeException("Error creating directory " + indexDirectory + " for index " + PROVIDER_DESCRIPTOR);
    }

    @Override
    public IndexPopulator getPopulator(long indexId, IndexDescriptor indexDescriptor, IndexConfiguration indexConfiguration, IndexSamplingConfig indexSamplingConfig) {
        final ChronicleIndex index = new ChronicleIndex(getIndexFile(indexId));
        this.indexes.put(indexId, index);
        return index;
    }

    @Override
    public IndexAccessor getOnlineAccessor(final long indexId, IndexConfiguration indexConfiguration, IndexSamplingConfig indexSamplingConfig) throws IOException {
        final ChronicleIndex index = this.indexes.get(indexId);
        if (index == null || index.getState() != InternalIndexState.ONLINE)
            throw new IllegalStateException("Index " + indexId + " not online yet");
        return index;
    }

    @Override
    public String getPopulationFailure(long l) throws IllegalStateException {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public InternalIndexState getInitialState(final long indexId) {
        final ChronicleIndex index = this.indexes.get(indexId);
        return index != null ? index.getState() : InternalIndexState.POPULATING;

    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant(final FileSystemAbstraction fs, UpgradableDatabase upgradableDatabase) {
        // taken from org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider#storeMigrationParticipant
        return new SchemaIndexMigrator( fs, upgradableDatabase, new SchemaIndexMigrator.SchemaStoreProvider()
        {
            @Override
            public SchemaStore provide( File dir, PageCache pageCache )
            {
                return new StoreFactory( fs, dir, pageCache, DEV_NULL, new Monitors(), ALLOW_OLD_VERSION ).newSchemaStore();
            }
        } );

    }

    @Override
    public void shutdown() throws Throwable {
        for (ChronicleIndex chronicleIndex : indexes.values()) {
            chronicleIndex.shutdown();
        }
        super.shutdown();
    }
}
