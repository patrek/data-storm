package datastorm.spring;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import datastorm.eventstore.otientdb.ThreadedODatabaseDocumentFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * @author EniSh
 *         Date: 10.04.11
 */
public class OrientTransactionManager extends AbstractPlatformTransactionManager {
    private ThreadedODatabaseDocumentFactory databaseFactory;

    @Override
    protected Object doGetTransaction() throws TransactionException {
        return new OrientTransactionObject(databaseFactory.getThreadLocalDatabase());
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        OrientTransactionObject txObj = (OrientTransactionObject) transaction;
        try {
            txObj.getDatabase().begin();
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

    private class OrientTransactionObject {
        private ODatabaseDocument database;

        private OrientTransactionObject(ODatabaseDocument database) {
            this.database = database;
        }

        public ODatabaseDocument getDatabase() {
            return database;
        }
    }
}
