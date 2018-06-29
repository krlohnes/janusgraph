package org.janusgraph.diskstorage.ignite;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.transactions.Transaction;
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

public class IgniteStoreManager extends DistributedStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(IgniteStoreManager.class);

    private final Map<String, IgniteKeyValueStore> stores;
    private final StoreFeatures features;
    private final Configuration config;

    private Ignite ignite;

    private Deployment deployment;
    private TcpDiscoveryConsulIpFinder tcpDCIF = new TcpDiscoveryConsulIpFinder();

    public static final ConfigNamespace IGNITE_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS,
                "ignite", "Apache Ignite configuration options");


    public static final ConfigOption<String> ISOLATION_LEVEL =
            new ConfigOption<>(IGNITE_NS, "isolation-level",
            "The isolation level used by transactions",
            ConfigOption.Type.MASKABLE,  String.class,
            TransactionIsolation.REPEATABLE_READ.toString(), disallowEmpty(String.class));

    public IgniteStoreManager(Configuration storageConfig) {
        this(storageConfig, 9500);
    }

    public IgniteStoreManager(Configuration storageConfig, int portDefault) {
        super(storageConfig, portDefault);
        stores = new HashMap<>();
        this.config = storageConfig;
        final String hostAddress;
        synchronized (IgniteStoreManager.class) {
            //DataStorageConfiguration storageCfg = new DataStorageConfiguration();
            //storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
            //storageCfg.setWalMode(WALMode.FSYNC);
            //storageCfg.setStoragePath("/tmp/ignite0");//TODO Add this to configuration
            final IgniteConfiguration igc = new IgniteConfiguration();
            //igc.setDataStorageConfiguration(storageCfg);
            try {
                hostAddress = InetAddress.getLocalHost().getHostAddress();
                TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi().setLocalAddress(hostAddress);
                discoverySpi.setIpFinder(tcpDCIF);
                igc.setDiscoverySpi(discoverySpi);
                Ignition.getOrStart(igc);
                ignite = Ignition.ignite();
            } catch (java.net.UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

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
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        Transaction tx = null;
        synchronized (this) {
            if (ignite.transactions().tx() == null) {
                tx = ignite.transactions().txStart();
                System.err.println("Starting tx " + tx.xid());
            } else {
                System.err.println("Tx found");
                tx = ignite.transactions().tx();
            }
        }
        return new IgniteTx(tx, config);
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
        IgniteKeyValueStore store = new IgniteKeyValueStore(ignite, name, config);
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
