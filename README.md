# Neo4j 2.0 Chronicle Index Provider

Implements a Schema Index Provider for Neo4j 2.0, label based indexes using [Chronicle-Map](http://chronicle.software/products/chronicle-map/), which is a high performance, off-heap, key-value, in memory, persisted data store.


`mvn clean install`

That will create a zip-file: `target/chronicle-index-1.0-provider.zip` whose content you have to put in Neo4j's classpath.