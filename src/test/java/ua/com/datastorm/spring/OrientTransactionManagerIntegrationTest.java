package ua.com.datastorm.spring;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import ua.com.datastorm.eventstore.orientdb.ConnectionManager;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;

import static org.mockito.Mockito.*;

/**
 * @author EniSh
 *         Date: 23.04.11
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@TestExecutionListeners({
        DependencyInjectionTestExecutionListener.class,
        TransactionalTestExecutionListener.class
})
public class OrientTransactionManagerIntegrationTest {
    @Autowired
    private OrientTransactionTester tester;
    @Autowired
    private StubConnectionManager connectionManager;

    @Before
    public void setUp() {
        reset(connectionManager.databaseMock);
    }

    @Test
    public void testCommit() {
        when(connectionManager.databaseMock.begin()).thenReturn(connectionManager.databaseMock);
        when(connectionManager.databaseMock.commit()).thenReturn(connectionManager.databaseMock);
        when(connectionManager.databaseMock.getTransaction()).thenReturn(new OTransactionNoTx(null));
        tester.saveData();
        verify(connectionManager.databaseMock).begin();
        verify(connectionManager.databaseMock).commit();
    }

    @Test
    public void testRollback() {
        when(connectionManager.databaseMock.begin()).thenReturn(connectionManager.databaseMock);
        when(connectionManager.databaseMock.getTransaction()).thenReturn(new OTransactionNoTx(null));
        when(connectionManager.databaseMock.rollback()).thenReturn(connectionManager.databaseMock);
        try {
            tester.throwException();
        } catch (RuntimeException e) {
            verify(connectionManager.databaseMock).begin();
            verify(connectionManager.databaseMock).rollback();
            return;
        }
        Assert.fail("Method must throw an exception");
    }

    @Transactional
    public static class OrientTransactionTester {
        @Autowired
        private ODatabaseDocument databaseDocument;

        public void saveData() {
            int i = 12;
            i++;
        }

        public void throwException() {
            throw new RuntimeException();
        }
    }

    public static class StubConnectionManager extends ConnectionManager {
        ODatabaseDocument databaseMock = Mockito.mock(ODatabaseDocument.class);

        public StubConnectionManager() {
            super("", "", "");
        }

        @Override
        public ODatabaseDocument getNewConnection() {
            return databaseMock;
        }
    }
}
