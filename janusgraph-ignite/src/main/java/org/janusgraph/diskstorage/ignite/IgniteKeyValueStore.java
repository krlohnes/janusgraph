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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
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
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

/**
 * Essentially a wrapper for operations on the IgniteCache
 */
public class IgniteKeyValueStore implements OrderedKeyValueStore {

    private IgniteCache<Long, ByteBuffer> kvStore;
    private static final int BSIZE = 1024;
    private static final String KEY_QUERY = "_key >= ? AND _key < ?";

    public IgniteKeyValueStore(Ignite ignite, String name, Configuration config) {
        final CacheConfiguration<Long, ByteBuffer> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(name);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setIndexedTypes(Long.class, ByteBuffer.class);
        kvStore = ignite.getOrCreateCache(cfg);
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        kvStore.remove(key.as(StaticBuffer.BB_FACTORY).getLong());
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        ByteBuffer value = kvStore.get(key.as(StaticBuffer.BB_FACTORY).getLong());
        if (value == null) {
            return null;
        } else {
            return StaticArrayBuffer.of(value);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return kvStore.containsKey(key.as(StaticBuffer.BB_FACTORY).getLong());
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
            //kvStore.close();
        }
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh)
            throws BackendException {
        System.err.println(key.as(StaticBuffer.BB_FACTORY).getLong());
        kvStore.put(key.as(StaticBuffer.BB_FACTORY).getLong(), value.as(StaticBuffer.BB_FACTORY));
    }

    //graph = JanusGraphFactory.open('conf/janusgraph-ignite-es.properties')

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh)
            throws BackendException {
        final KeySelector selector = query.getKeySelector();
        final Long start = query.getStart().as(StaticBuffer.BB_FACTORY).getLong();
        Long possibleEnd = query.getEnd().as(StaticBuffer.BB_FACTORY).getLong();
        if (possibleEnd < 0) {
            possibleEnd = Long.MAX_VALUE;
        }
        final Long end = possibleEnd;

        //SqlQuery<Long, ByteBuffer> sqlQuery = new SqlQuery<>(ByteBuffer.class, KEY_QUERY);
        //sqlQuery.setArgs(start, end); Keep an eye on
        //https://issues.apache.org/jira/browse/IGNITE-4191
        //So this can be done more efficiently and correctly
        //Right now this doesn't meet JG standards for storage retrieval. It will after 4191
        final List<KeyValueEntry> result = new ArrayList<>();
        int i = 0;
        ScanQuery<Long, ByteBuffer> sq = new ScanQuery<>((k, p) -> k > start && k <= end);
        try (QueryCursor<Entry<Long, ByteBuffer>> cursor = kvStore.query(sq)) {
           for (Entry<Long, ByteBuffer> e : cursor) {
               i++;
               ByteBuffer bb = ByteBuffer.allocate(BSIZE);
               bb.asLongBuffer().put(e.getKey());
               if (selector.include(StaticArrayBuffer.of(bb))) {
                    result.add(new KeyValueEntry(StaticArrayBuffer.of(bb),
                               StaticArrayBuffer.of(e.getValue())));
               }
               if (selector.reachedLimit()) break;
            }
            System.err.println("found: " + i);
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
