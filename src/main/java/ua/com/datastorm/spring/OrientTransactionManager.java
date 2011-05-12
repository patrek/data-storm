package ua.com.datastorm.spring;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import ua.com.datastorm.eventstore.orientdb.ConnectionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * <p> {@link org.springframework.transaction.PlatformTransactionManager} implementation for OrientDb </p>
 *
 * @author EniSh
 */
public class OrientTransactionManager extends AbstractPlatformTransactionManager {
    private ConnectionManager connectionManager;

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        OrientTransactionObject transactionObject = new OrientTransactionObject();
        ODatabaseDocument connection = (ODatabaseDocument) TransactionSynchronizationManager.getResource(connectionManager);
        if (connection != null) {
            transactionObject.setDatabase(connection, false);
        }
        return transactionObject;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        OrientTransactionObject txObj = (OrientTransactionObject) transaction;
        try {
            if (txObj.getDatabase() == null) {
                txObj.setDatabase(connectionManager.getNewConnection(), true);
            }

            txObj.getDatabase().begin();

            if (txObj.isConnectionNew()) {
                TransactionSynchronizationManager.bindResource(connectionManager, txObj.getDatabase());
            }
        } catch (RuntimeException e) {
            //TODO translate e
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        OrientTransactionObject transaction = (OrientTransactionObject) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Committing Orient transaction on ODatabaseDocument [" +
                    transaction.getDatabase() + "]");
        }
        try {
            transaction.getDatabase().commit();
        } catch (RuntimeException e) {
            //TODO create exception translator
            //DataAccessUtils.translateIfNecessary(e, translator)
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        OrientTransactionObject transaction = (OrientTransactionObject) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Rolling back Orient transaction on ODatabaseDocument [" +
                    transaction.getDatabase() + "]");
        }
        try {
            transaction.getDatabase().rollback();
        } catch (RuntimeException e) {
            //TODO create exception translator
            //DataAccessUtils.translateIfNecessary(e, translator)
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        OrientTransactionObject tx = (OrientTransactionObject) transaction;
        if (tx.isConnectionNew()) {
            TransactionSynchronizationManager.unbindResource(connectionManager);
            tx.getDatabase().close();
        }
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        return ((OrientTransactionObject) transaction).hasTransaction();
    }

    private class OrientTransactionObject {
        private ODatabaseDocument database;
        private boolean connectionNew;

        private OrientTransactionObject() {
        }

        public ODatabaseDocument getDatabase() {
            return database;
        }

        public boolean hasTransaction() {
            return (database != null) && !(database.getTransaction() instanceof OTransactionNoTx);
        }

        public void setDatabase(final ODatabaseDocument database, boolean newConnection) {
            this.database = database;
            this.connectionNew = newConnection;
        }

        public boolean isConnectionNew() {
            return connectionNew;
        }
    }
}
