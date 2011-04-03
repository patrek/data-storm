package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Before;
import org.junit.Test;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.agId;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Unit test for {@link PerInstanceClusterResolver}
 *
 * @author Andrey Lomakin
 */
public class PerInstanceClusterResolverTest {
    private ODatabaseDocument database = mock(ODatabaseDocument.class);
    private PerInstanceClusterResolver clusterResolver = new PerInstanceClusterResolver();

    @Before
    public void setUp() {
        reset(database);
        clusterResolver.setDatabase(database);
    }

    @Test
    public void testClusterCreation() {
        when(database.getClusterIdByName("Simple.1")).thenReturn(-1);

        final String clusterName = clusterResolver.resolveClusterForAggregate("Simple", agId("1"));

        assertEquals("Simple.1", clusterName);
        verify(database).getClusterIdByName("Simple.1");
        verify(database).addPhysicalCluster("Simple.1", "Simple.1", -1);
    }

    @Test
    public void testClusterResolution() {
        when(database.getClusterIdByName("Simple.1")).thenReturn(1);

        final String clusterName = clusterResolver.resolveClusterForAggregate("Simple", agId("1"));

        assertEquals("Simple.1", clusterName);
        verify(database).getClusterIdByName("Simple.1");
    }
}
