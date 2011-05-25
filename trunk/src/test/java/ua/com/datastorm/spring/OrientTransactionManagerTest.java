package ua.com.datastorm.spring;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import ua.com.datastorm.eventstore.orientdb.ConnectionManager;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author EniSh
 *         Date: 15.05.11
 */
public class OrientTransactionManagerTest {
    private OrientTransactionManager transactionManager;
    private ConnectionManager connectionManagerMock;
    private ODatabaseDocument connectionMock;
    private OrientPersistenceExceptionTranslator exceptionTranslatorMock;

    @Before
    public void setUp() throws Exception {
        connectionManagerMock = mock(ConnectionManager.class);
        exceptionTranslatorMock = mock(OrientPersistenceExceptionTranslator.class);
        transactionManager = new OrientTransactionManager();
        transactionManager.setConnectionManager(connectionManagerMock);
        transactionManager.setPersistenceExceptionTranslator(exceptionTranslatorMock);
        connectionMock = mock(ODatabaseDocument.class);
    }

    @After
    public void tearDown() {
        TransactionSynchronizationManager.unbindResourceIfPossible(connectionManagerMock);
    }

    @Test
    public void testDoGetTransaction() {
        TransactionSynchronizationManager.bindResource(connectionManagerMock, connectionMock);

        OrientTransactionManager.OrientTransactionObject orientTransactionObject = (OrientTransactionManager.OrientTransactionObject) transactionManager.doGetTransaction();
        assertEquals("Transaction object have wrong connection", connectionMock, orientTransactionObject.getDatabase());
        assertFalse("Connection must be marked as not new", orientTransactionObject.isConnectionNew());

        TransactionSynchronizationManager.unbindResource(connectionManagerMock);
    }

    @Test
    public void testDoGetTransactionNoConnection() {
        OrientTransactionManager.OrientTransactionObject transactionObjectNoTransaction = (OrientTransactionManager.OrientTransactionObject) transactionManager.doGetTransaction();
        assertNull("Transaction object without transaction must have no database", transactionObjectNoTransaction.getDatabase());
    }

    @Test
    public void testDoBegin() {
        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        when(connectionMock.begin()).thenReturn(connectionMock);

        transactionManager.doBegin(txObject, null);

        verify(connectionMock).begin();
    }

    @Test
    public void testDoBeginRuntimeException() {
        RuntimeException error = new RuntimeException("ERROR");
        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        when(connectionMock.begin()).thenThrow(error);
        when(connectionMock.rollback()).thenReturn(connectionMock);
        when(connectionMock.getTransaction()).thenReturn(new OTransactionOptimistic(null));
        doNothing().when(connectionMock).close();

        boolean caught = false;
        try {
            transactionManager.doBegin(txObject, null);
        } catch (CannotCreateTransactionException e) {
            caught = true;
        }

        verify(connectionMock).begin();
        verify(connectionMock).rollback();
        verify(connectionMock).close();

        assertTrue("Exception must be caught", caught);
    }

    @Test
    public void testDoBeginRuntimeExceptionWithoutConnection() {
        RuntimeException error = new RuntimeException("ERROR");
        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        when(connectionMock.begin()).thenThrow(error);
        when(connectionMock.getTransaction()).thenReturn(new OTransactionNoTx(null));
        doNothing().when(connectionMock).close();

        boolean caught = false;
        try {
            transactionManager.doBegin(txObject, null);
        } catch (CannotCreateTransactionException e) {
            caught = true;
        }

        verify(connectionMock).begin();
        verify(connectionMock).close();

        assertTrue("Exception must be caught", caught);
    }

    @Test
    public void testDoBeginNoConnection() {
        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        when(connectionManagerMock.getNewConnection()).thenReturn(connectionMock);
        when(connectionMock.begin()).thenReturn(connectionMock);

        transactionManager.doBegin(txObject, null);

        verify(connectionManagerMock).getNewConnection();
        verify(connectionMock).begin();
        assertEquals("Connection in txObject not same as expected", connectionMock, txObject.getDatabase());

        assertEquals("Connection must be registered in transaction synchronization manager",
                connectionMock,
                TransactionSynchronizationManager.getResource(connectionManagerMock));

        TransactionSynchronizationManager.unbindResource(connectionManagerMock);
    }

    @Test
    public void testDoCommit() {
        when(connectionMock.commit()).thenReturn(connectionMock);

        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        DefaultTransactionStatus status = new DefaultTransactionStatus(txObject, false, false, false, false, null);

        transactionManager.doCommit(status);

        verify(connectionMock).commit();
    }


    @Test
    public void testDoCommitRuntimeException() {
        RuntimeException error = new RuntimeException("ERROR");
        when(connectionMock.commit()).thenThrow(error);
        when(exceptionTranslatorMock.translateExceptionIfPossible(error)).thenReturn(new InvalidDataAccessApiUsageException(""));

        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        DefaultTransactionStatus status = new DefaultTransactionStatus(txObject, false, false, false, false, null);

        boolean caught = false;
        try {
            transactionManager.doCommit(status);
        } catch (InvalidDataAccessApiUsageException e) {
            caught = true;
        }

        verify(connectionMock).commit();
        verify(exceptionTranslatorMock).translateExceptionIfPossible(error);

        assertTrue("Exception must be caught", caught);
    }

    @Test
    public void testDoRollback() {
        when(connectionMock.rollback()).thenReturn(connectionMock);

        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        DefaultTransactionStatus status = new DefaultTransactionStatus(txObject, false, false, false, false, null);

        transactionManager.doRollback(status);

        verify(connectionMock).rollback();
    }

    @Test
    public void testDoRollbackRuntimeException() {
        RuntimeException error = new RuntimeException("ERROR");
        when(connectionMock.rollback()).thenThrow(error);

        when(exceptionTranslatorMock.translateExceptionIfPossible(error)).thenReturn(new InvalidDataAccessApiUsageException(""));


        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        DefaultTransactionStatus status = new DefaultTransactionStatus(txObject, false, false, false, false, null);

        boolean caught = false;
        try {
            transactionManager.doRollback(status);
        } catch (InvalidDataAccessApiUsageException e) {
            caught = true;
        }

        verify(connectionMock).rollback();
        verify(exceptionTranslatorMock).translateExceptionIfPossible(error);

        assertTrue("Exception must be caught", caught);
    }

    @Test
    public void testDoCleanUpAfterCompletion() {
        doNothing().when(connectionMock).close();

        TransactionSynchronizationManager.bindResource(connectionManagerMock, connectionMock);

        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, true);
        transactionManager.doCleanupAfterCompletion(txObject);

        assertFalse("Connection must be unbound", TransactionSynchronizationManager.hasResource(connectionManagerMock));
        verify(connectionMock).close();
    }

    @Test
    public void testDoCleanUpAfterCompletionNotNewTx() {
        TransactionSynchronizationManager.bindResource(connectionManagerMock, connectionMock);

        OrientTransactionManager.OrientTransactionObject txObject = transactionManager.new OrientTransactionObject();
        txObject.setDatabase(connectionMock, false);
        transactionManager.doCleanupAfterCompletion(txObject);

        assertTrue("Connection mustn't be unbound", TransactionSynchronizationManager.hasResource(connectionManagerMock));
    }
}
