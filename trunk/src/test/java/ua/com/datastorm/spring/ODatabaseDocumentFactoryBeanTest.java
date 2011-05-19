package ua.com.datastorm.spring;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.FactoryBeanNotInitializedException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import ua.com.datastorm.eventstore.orientdb.ConnectionManager;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Unit test for {@link ODatabaseDocumentFactoryBean}
 * @author Artem Orobets enisher@gmail.com
 */
public class ODatabaseDocumentFactoryBeanTest {
    private ODatabaseDocumentFactoryBean factoryBean;

    @Before
    public void setUp() throws Exception {
        factoryBean = new ODatabaseDocumentFactoryBean();
    }

    @Test(expected = FactoryBeanNotInitializedException.class)
    public void testGetObjectConnectionManagerIsNull() {
        ODatabaseDocument object = factoryBean.getObject();
    }

    @Test
    public void testProxyInTransaction() throws Exception {
        ConnectionManager connectionManager = mock(ConnectionManager.class);
        ODatabaseDocument connection = mock(ODatabaseDocument.class);

        when(connection.begin()).thenReturn(null);

        TransactionSynchronizationManager.bindResource(connectionManager, connection);

        factoryBean.setConnectionManager(connectionManager);
        ODatabaseDocument database = factoryBean.getObject();

        database.begin();

        verify(connection).begin();
    }

    @Test(expected = IllegalStateException.class)
    public void testProxyOutTransaction() throws Exception {
        factoryBean.setConnectionManager(mock(ConnectionManager.class));
        ODatabaseDocument database = factoryBean.getObject();

        database.begin();
    }

    @Test
    public void testGetObjectType() throws Exception {
        assertEquals( "Factory must generate proxy of ODatabaseDocument", factoryBean.getObjectType(), ODatabaseDocument.class );
    }

    @Test
    public void testIsSingleton() throws Exception {
        assertTrue( "Factory must generate singleton" , factoryBean.isSingleton() );
    }
}
