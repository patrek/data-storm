package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Before;
import org.junit.Test;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.agId;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link PerTypeClusterResolver}
 *
 * @author Andrey Lomakin
 */
public class PerTypeClusterResolverTest {
    private ODatabaseDocument database = mock(ODatabaseDocument.class);
    private PerTypeClusterResolver clusterResolver = new PerTypeClusterResolver();

    @Before
    public void setUp() {
        reset(database);
        clusterResolver.setDatabase(database);
    }

    @Test
    public void testClusterCreation() {
        when(database.getClusterIdByName("Simple")).thenReturn(-1);

        final String clusterName = clusterResolver.resolveClusterForAggregate("Simple", agId("1"));

        assertEquals("Simple", clusterName);
        verify(database).getClusterIdByName("Simple");
        verify(database).addPhysicalCluster("Simple", "Simple", -1);
    }

    @Test
    public void testClusterResolution() {
        when(database.getClusterIdByName("Simple")).thenReturn(1);

        final String clusterName = clusterResolver.resolveClusterForAggregate("Simple", agId("1"));

        assertEquals("Simple", clusterName);
        verify(database).getClusterIdByName("Simple");
    }
}
