package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CacheTransaction implements StoreTransaction, LoggableTransaction {

    private static final Logger log =
            LoggerFactory.getLogger(CacheTransaction.class);

    private final StoreTransaction tx;
    private final KeyColumnValueStoreManager manager;
    private final boolean batchLoading;
    private final int persistChunkSize;
    private final Duration maxWriteTime;

    private int numMutations;
    private final Map<KCVSCache, Map<StaticBuffer, KCVEntryMutation>> mutations;

    public CacheTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager,
                             int persistChunkSize, Duration maxWriteTime, boolean batchLoading) {
        this(tx, manager, persistChunkSize, maxWriteTime, batchLoading, 2);
    }

    public CacheTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager, int persistChunkSize,
                            Duration maxWriteTime, boolean batchLoading, int expectedNumStores) {
        Preconditions.checkArgument(tx != null && manager != null && persistChunkSize > 0);
        this.tx = tx;
        this.manager = manager;
        this.batchLoading = batchLoading;
        this.numMutations = 0;
        this.persistChunkSize = persistChunkSize;
        this.maxWriteTime = maxWriteTime;
        this.mutations = new HashMap<KCVSCache, Map<StaticBuffer, KCVEntryMutation>>(expectedNumStores);
    }

    public StoreTransaction getWrappedTransaction() {
        return tx;
    }

    void mutate(KCVSCache store, StaticBuffer key, List<Entry> additions, List<Entry> deletions) throws BackendException {
        Preconditions.checkNotNull(store);
        if (additions.isEmpty() && deletions.isEmpty()) return;

        KCVEntryMutation m = new KCVEntryMutation(additions, deletions);
        Map<StaticBuffer, KCVEntryMutation> storeMutation = mutations.get(store);
        if (storeMutation == null) {
            storeMutation = new HashMap<StaticBuffer, KCVEntryMutation>();
            mutations.put(store, storeMutation);
        }
        KCVEntryMutation existingM = storeMutation.get(key);
        if (existingM != null) {
            existingM.merge(m);
        } else {
            storeMutation.put(key, m);
        }

        numMutations += m.getTotalMutations();

        if (batchLoading && numMutations >= persistChunkSize) {
            flushInternal();
        }
    }

    private int persist(final Map<String, Map<StaticBuffer, KCVMutation>> subMutations) {
        BackendOperation.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                manager.mutateMany(subMutations, tx);
                return true;
            }

            @Override
            public String toString() {
                return "CacheMutation";
            }
        }, maxWriteTime);
        subMutations.clear();
        return 0;
    }

    private int mutationSize(Map<StaticBuffer, KCVEntryMutation> mutation) {
        int size = 0;
        for (KCVEntryMutation mut : mutation.values()) size+=mut.getTotalMutations();
        return size;
    }

    private KCVMutation convert(KCVEntryMutation mutation) {
        assert !mutation.isEmpty();
        if (!mutation.hasDeletions())
            return new KCVMutation(mutation.getAdditions(), KeyColumnValueStore.NO_DELETIONS);
        else
            return new KCVMutation(mutation.getAdditions(), Lists.newArrayList(Iterables.transform(mutation.getDeletions(), KCVEntryMutation.ENTRY2COLUMN_FCT)));
    }

    private void flushInternal() throws BackendException {
        if (numMutations > 0) {
            //Consolidate all mutations prior to persistence to ensure that no addition accidentally gets swallowed by a delete
            for (Map<StaticBuffer, KCVEntryMutation> store : mutations.values()) {
                for (KCVEntryMutation mut : store.values()) mut.consolidate();
            }

            //Chunk up mutations
            final Map<String, Map<StaticBuffer, KCVMutation>> subMutations = new HashMap<String, Map<StaticBuffer, KCVMutation>>(mutations.size());
            int numSubMutations = 0;
            for (Map.Entry<KCVSCache,Map<StaticBuffer, KCVEntryMutation>> storeMuts : mutations.entrySet()) {
                Map<StaticBuffer, KCVMutation> sub = new HashMap<StaticBuffer, KCVMutation>();
                subMutations.put(storeMuts.getKey().getName(),sub);
                for (Map.Entry<StaticBuffer,KCVEntryMutation> muts : storeMuts.getValue().entrySet()) {
                    if (muts.getValue().isEmpty()) continue;
                    sub.put(muts.getKey(), convert(muts.getValue()));
                    numSubMutations+=muts.getValue().getTotalMutations();
                    if (numSubMutations>= persistChunkSize) {
                        numSubMutations = persist(subMutations);
                        sub.clear();
                        subMutations.put(storeMuts.getKey().getName(),sub);
                    }
                }
            }
            if (numSubMutations>0) persist(subMutations);


            for (Map.Entry<KCVSCache,Map<StaticBuffer, KCVEntryMutation>> storeMuts : mutations.entrySet()) {
                KCVSCache cache = storeMuts.getKey();
                for (Map.Entry<StaticBuffer,KCVEntryMutation> muts : storeMuts.getValue().entrySet()) {
                    if (cache.hasValidateKeysOnly()) {
                        cache.invalidate(muts.getKey(), Collections.EMPTY_LIST);
                    } else {
                        KCVEntryMutation m = muts.getValue();
                        List<CachableStaticBuffer> entries = new ArrayList<CachableStaticBuffer>(m.getTotalMutations());
                        for (Entry e : m.getAdditions()) {
                            assert e instanceof CachableStaticBuffer;
                            entries.add((CachableStaticBuffer)e);
                        }
                        for (StaticBuffer e : m.getDeletions()) {
                            assert e instanceof CachableStaticBuffer;
                            entries.add((CachableStaticBuffer)e);
                        }
                        cache.invalidate(muts.getKey(),entries);
                    }
                }
            }
            clear();
        }
    }

    private void clear() {
        for (Map.Entry<KCVSCache, Map<StaticBuffer, KCVEntryMutation>> entry : mutations.entrySet()) {
            entry.getValue().clear();
        }
        numMutations = 0;
    }

    @Override
    public void logMutations(DataOutput out) {
        Preconditions.checkArgument(!batchLoading,"Cannot log entire mutation set when batch-loading is enabled");
        VariableLong.writePositive(out,mutations.size());
        for (Map.Entry<KCVSCache,Map<StaticBuffer, KCVEntryMutation>> storeMuts : mutations.entrySet()) {
            out.writeObjectNotNull(storeMuts.getKey().getName());
            VariableLong.writePositive(out,storeMuts.getValue().size());
            for (Map.Entry<StaticBuffer,KCVEntryMutation> muts : storeMuts.getValue().entrySet()) {
                BufferUtil.writeBuffer(out,muts.getKey());
                KCVEntryMutation mut = muts.getValue();
                logMutatedEntries(out,mut.getAdditions());
                logMutatedEntries(out,mut.getDeletions());
            }
        }
    }

    private void logMutatedEntries(DataOutput out, List<Entry> entries) {
        VariableLong.writePositive(out,entries.size());
        for (Entry add : entries) BufferUtil.writeEntry(out,add);
    }

    @Override
    public void commit() throws BackendException {
        flushInternal();
        tx.commit();
    }

    @Override
    public void rollback() throws BackendException {
        clear();
        tx.rollback();
    }

    @Override
    public BaseTransactionConfig getConfiguration() {
        return tx.getConfiguration();
    }

}
