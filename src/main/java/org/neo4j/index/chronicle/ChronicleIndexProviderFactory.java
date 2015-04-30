package org.neo4j.index.chronicle;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

@Service.Implementation(KernelExtensionFactory.class)
public class ChronicleIndexProviderFactory extends KernelExtensionFactory<ChronicleIndexProviderFactory.Dependencies> {
    public static final String KEY = "chronicle-index";

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor(KEY, "1.0");

    private final ChronicleSchemaIndexProvider singleProvider;

    public interface Dependencies {
        Config getConfig();
    }

    public ChronicleIndexProviderFactory() {
        this(null);
    }

    public ChronicleIndexProviderFactory(ChronicleSchemaIndexProvider singleProvider) {
        super(KEY);
        this.singleProvider = singleProvider;
    }

    @Override
    public Lifecycle newKernelExtension(Dependencies dependencies) throws Throwable {
        return hasSingleProvider() ? singleProvider : new ChronicleSchemaIndexProvider(dependencies.getConfig());
    }

    private boolean hasSingleProvider() {
        return singleProvider != null;
    }
}
