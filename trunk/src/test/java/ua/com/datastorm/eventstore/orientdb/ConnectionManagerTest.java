package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author EniSh
 *         Date: 15.05.11
 */
public class ConnectionManagerTest {
    private ConnectionManager connectionManager;
    private ODatabaseDocumentTx expectedConnection = mock(ODatabaseDocumentTx.class);
    private String expectedDatabaseUrl = "databaseUrl";
    private String expectedUserName = "userName";
    private String expectedPassword = "databasePass";
    private int expectedMinPoolSize = 10;
    private int expectedMaxPoolSize = 50;

    @Test
    public void testGetNewConnection() {
        ODatabaseDocumentPool poolMock = mock(ODatabaseDocumentPool.class);

        when(poolMock.acquire(expectedDatabaseUrl, expectedUserName, expectedPassword)).thenReturn(expectedConnection);

        connectionManager = new ConnectionManager(expectedDatabaseUrl, expectedUserName, expectedPassword, poolMock);

        ODatabaseDocument newConnection = connectionManager.getNewConnection();

        assertEquals("Created connection not same as expected.", expectedConnection, newConnection);

        verify(poolMock).acquire(expectedDatabaseUrl, expectedUserName, expectedPassword);
        verifyZeroInteractions(expectedConnection);
    }
}
