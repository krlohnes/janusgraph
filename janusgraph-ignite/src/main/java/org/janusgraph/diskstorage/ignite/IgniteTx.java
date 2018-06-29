package org.janusgraph.diskstorage.ignite;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

public class IgniteTx extends AbstractStoreTransaction {

    private volatile Transaction tx;
    public IgniteTx(Transaction t, BaseTransactionConfig config) {
        super(config);
        tx = t;
    }

    @Override
    public synchronized void commit() {
        if (tx == null) {
            return;
        } else {
            System.err.println("Committing tx " + tx.xid());
            tx.commit();
        }
    }

    @Override
    public synchronized void rollback() {
        if (tx == null) {
            return;
        } else {
            System.err.println("Rolling back tx " + tx.xid());
            tx.rollback();
        }
    }

    public IgniteUuid getId() {
        return tx.xid();
    }

}
