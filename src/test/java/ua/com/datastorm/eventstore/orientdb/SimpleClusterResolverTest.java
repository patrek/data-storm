package ua.com.datastorm.eventstore.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.agId;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link SimpleClusterResolver}.
 *
 * @author Andrey Lomakin
 */
@RunWith(value = Parameterized.class)
public class SimpleClusterResolverTest {
    private ODatabaseDocument database = mock(ODatabaseDocument.class);
    private SimpleClusterResolver clusterResolver = new SimpleClusterResolver("TestCluster");


    private String clusterName;
    private String typeName;
    private String agId;

    /**
     * @return Cluster Name, Type Name, Aggregate Id
     */
    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(
                new Object[]{
                        "TestCluster", "Simple", "1"
                },
                new Object[]{
                        "TestCluster", "Simple", "2"
                }
        );
    }

    public SimpleClusterResolverTest(String clusterName, String typeName, String agId) {
        this.clusterName = clusterName;
        this.typeName = typeName;
        this.agId = agId;
    }


    @Before
    public void setUp() {
        reset(database);
        clusterResolver.setDatabase(database);
    }

    @Test
    public void testClusterCreation() {
        when(database.getClusterIdByName(clusterName)).thenReturn(-1);

        final String resultClusterName = clusterResolver.resolveClusterForAggregate(typeName, agId(agId));

        assertEquals(clusterName, resultClusterName);
        verify(database).getClusterIdByName(clusterName);
        verify(database).addPhysicalCluster(clusterName, clusterName, -1);
    }

    @Test
    public void testClusterResolution() {
        when(database.getClusterIdByName(clusterName)).thenReturn(1);

        final String resultClusterName = clusterResolver.resolveClusterForAggregate(typeName, agId(agId));

        assertEquals(clusterName, resultClusterName);
        verify(database).getClusterIdByName(clusterName);
    }
}
