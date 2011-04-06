package datastorm.eventstore.otientdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClassHasClusterIds;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClusterNames;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests storing and reading events in case when
 * {@link ClusterResolver} is {@link PerTypeClusterResolver} .
 *
 * @author Andrey Lomakin
 */
public class OrientEventStoreSimpleClusterTest extends OrientEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final SimpleClusterResolver clusterResolver = new SimpleClusterResolver("BigCluster");
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
        beforeClusters = database.getClusterNames();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"bigcluster"});
        assertClassHasClusterIds(new String[]{"bigcluster"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
        super.tearDown();
    }
}
