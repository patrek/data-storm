package datastorm.eventstore.otientdb;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClusterNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests storing and reading events in case when
 * {@link ClusterResolver} is {@link PerInstanceClusterResolver} .
 *
 * @author Andrey Lomakin
 */
public class OrientEventStorePerInstanceClusterTest extends OrientEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final PerInstanceClusterResolver clusterResolver = new PerInstanceClusterResolver();
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
        beforeClusters = database.getClusterNames();
    }

    @Override
    @Test
    public void testBasicEventsStoring() throws Exception {
        super.testBasicEventsStoring();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"simple.1"});
    }

    @Override
    @Test
    public void testEventsFromDifferentTypesWithSameId() {
        super.testEventsFromDifferentTypesWithSameId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"docone.1", "doctwo.1"});
    }

    @Override
    @Test
    public void testEventsFromDifferentTypesWithDiffId() {
        super.testEventsFromDifferentTypesWithDiffId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"docone.1", "doctwo.2"});
    }

    @Override
    @Test
    public void testEventsWithDiffId() {
        super.testEventsWithDiffId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"doc.1", "doc.2"});
    }
}
