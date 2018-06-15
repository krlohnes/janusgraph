package org.janusgraph.diskstorage.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.transactions.Transaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

public class IgniteTx extends AbstractStoreTransaction {

    //TODO This will need to change if non transactional use
    //is desired
    private static Ignite ignite = Ignition.ignite();
    private static IgniteTransactions transactions = ignite.transactions();
    private Transaction tx;
    public IgniteTx(BaseTransactionConfig config) {
        super(config);
        tx = transactions.txStart();
    }

    @Override
    public void commit() {
        tx.commit();
    }

    @Override
    public void rollback() {
        tx.rollback();
    }

}
