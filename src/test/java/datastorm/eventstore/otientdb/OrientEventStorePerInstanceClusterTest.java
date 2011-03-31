package datastorm.eventstore.otientdb;

import org.junit.Before;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests storing and reading events in case when
 * {@link ClusterResolver} is {@link PerInstanceClusterResolver} .
 *
 * @author Andrey Lomakin
 */
public class OrientEventStorePerInstanceClusterTest extends OrientEventStoreTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final PerInstanceClusterResolver clusterResolver = new PerInstanceClusterResolver();
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
    }
}
