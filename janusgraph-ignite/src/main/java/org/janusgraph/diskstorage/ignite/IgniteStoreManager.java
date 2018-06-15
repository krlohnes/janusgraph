package org.janusgraph.diskstorage.ignite;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.transactions.TransactionIsolation;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteStoreManager extends DistributedStoreManager
        implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(IgniteStoreManager.class);

    private final Map<String, IgniteKeyValueStore> stores;
    private final StoreFeatures features;
    private final Configuration config;

    private Deployment deployment;

    public static final ConfigNamespace IGNITE_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS,
                "ignite", "Apache Ignite configuration options");


    public static final ConfigOption<String> ISOLATION_LEVEL =
            new ConfigOption<>(IGNITE_NS, "isolation-level",
            "The isolation level used by transactions",
            ConfigOption.Type.MASKABLE,  String.class,
            TransactionIsolation.REPEATABLE_READ.toString(), disallowEmpty(String.class));

    public IgniteStoreManager(Configuration storageConfig, int portDefault) {
        super(storageConfig, portDefault);
        stores = new HashMap<>();
        this.config = storageConfig;

        features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(transactional)
                    .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .scanTxConfig(GraphDatabaseConfiguration.buildGraphConfiguration()
                            .set(ISOLATION_LEVEL, TransactionIsolation.REPEATABLE_READ.toString()))
                    .supportsInterruption(false)
                    .optimisticLocking(false)
                    .build();
          this.deployment = Deployment.LOCAL;
//        features = new StoreFeatures();
//        features.supportsOrderedScan = true;
//        features.supportsUnorderedScan = false; This _could_ be true
//        features.supportsBatchMutation = false;
//        features.supportsTxIsolation = transactional;
//        features.supportsConsistentKeyOperations = true;
//        features.supportsLocking = true;
//        features.isKeyOrdered = true;
//        features.isDistributed = true;
//        features.hasLocalKeyPartition = false;
//          Not sure yet about this. I think false is correct.
//        features.supportsMultiQuery = false;
        // TODO Auto-generated constructor stub
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new IgniteTx(config);
    }

    @Override
    public void close() throws BackendException {
        for (Map.Entry<String, IgniteKeyValueStore> igniteKeyValueStore : stores.entrySet()) {
            igniteKeyValueStore.getValue().close();
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        for (Map.Entry<String, IgniteKeyValueStore> igniteKeyValueStore : stores.entrySet()) {
            igniteKeyValueStore.getValue().clear();
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return stores.isEmpty();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getName() {
        // TODO Not sure about what this name should be yet... probably just the name provided by
        // the configuration?
        if(config.has(GRAPH_NAME)) return config.get(GRAPH_NAME);

        return "janusgraph";
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        //This may be supportable, not sure yet
        throw new UnsupportedOperationException();
    }

    @Override
    public OrderedKeyValueStore openDatabase(String name) throws BackendException {
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        IgniteKeyValueStore store = new IgniteKeyValueStore(name, config);
        stores.put(name, store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh)
            throws BackendException {
        for (Map.Entry<String,KVMutation> mutation : mutations.entrySet()) {
            IgniteKeyValueStore store = (IgniteKeyValueStore) openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();
            if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }

            if (mutationValue.hasAdditions()) {
                for (KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }
            if (mutationValue.hasDeletions()) {
                for (StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }

    @Override
    public Deployment getDeployment() {
        return deployment;
    }

}
