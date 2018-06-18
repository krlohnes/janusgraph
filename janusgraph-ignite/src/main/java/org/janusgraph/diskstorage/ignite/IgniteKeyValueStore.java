// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.ignite;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;

/**
 * Essentially a wrapper for operations on the IgniteCache
 */
public class IgniteKeyValueStore implements OrderedKeyValueStore {

    private IgniteCache<StaticBuffer, StaticBuffer> kvStore;
    private TcpDiscoveryConsulIpFinder tcpDCIF = new TcpDiscoveryConsulIpFinder();
    private static final String KEY_QUERY = "_key >= ? AND _key <= ? ORDER BY _key ASC";

    public IgniteKeyValueStore(String name, Configuration config) {
        //TODO configure number of replicas via configuration
        //TODO configure durable storage
        final IgniteConfiguration igc = new IgniteConfiguration();
        final CacheConfiguration<StaticBuffer, StaticBuffer> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(name);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setIndexedTypes(StaticBuffer.class, StaticBuffer.class);
        String hostAddress;
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
            TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi().setLocalAddress(hostAddress);
            discoverySpi.setIpFinder(tcpDCIF);
            igc.setDiscoverySpi(discoverySpi);
            Ignite ignite = Ignition.getOrStart(igc);
            kvStore = ignite.getOrCreateCache(cfg);
        } catch (java.net.UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        kvStore.remove(key);
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return kvStore.get(key);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return kvStore.containsKey(key);
    }

    public void clear() {
        kvStore.removeAll();
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh)
            throws BackendException {
        // TODO Do nothing for now. Rethink this later
    }

    @Override
    public String getName() {
        return kvStore.getName();
    }

    @Override
    public void close() throws BackendException {
        if (kvStore.isClosed()) {
            throw new PermanentBackendException("Store already closed");
        } else {
            kvStore.close();
        }
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh)
            throws BackendException {
        kvStore.put(key, value);
    }

    //graph = JanusGraphFactory.open('conf/janusgraph-ignite-es.properties')

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh)
            throws BackendException {
        final KeySelector selector = query.getKeySelector();
        final SqlQuery<StaticBuffer, StaticBuffer> sqlQuery =
            new SqlQuery<StaticBuffer, StaticBuffer>(StaticBuffer.class, KEY_QUERY);
        sqlQuery.setArgs(query.getStart(), query.getEnd());
        final List<KeyValueEntry> result = new ArrayList<>();
        try (QueryCursor<Entry<StaticBuffer, StaticBuffer>> cursor = kvStore.query(sqlQuery)) {
            for (Entry<StaticBuffer, StaticBuffer> e : cursor) {
               if (selector.include(e.getKey())) {
                    result.add(new KeyValueEntry(e.getKey(), e.getValue()));
               }
               if (selector.reachedLimit()) break;
            }
        }
        return recordIteratorFromList(result);
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries,
            StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    private RecordIterator<KeyValueEntry> recordIteratorFromList(List<KeyValueEntry> list) {
        return new RecordIterator<KeyValueEntry>() {
            private final Iterator<KeyValueEntry> entries = list.iterator();

            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public KeyValueEntry next() {
                return entries.next();
            }

            @Override
            public void close() {
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }


}
