package datastorm.eventstore.otientdb;

import org.junit.Before;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests storing and reading events in case when
 * {@link ClusterResolver} is {@link PerTypeClusterResolver} .
 *
 * @author Andrey Lomakin
 */
public class OrientEventStoreSimpleClusterTest extends OrientEventStoreTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final SimpleClusterResolver clusterResolver = new SimpleClusterResolver("BigCluster");
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
    }
}
