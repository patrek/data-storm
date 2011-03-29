package datastorm.eventstore.otientdb;

import org.junit.Before;

/**
 *
 */
public class OrientEventStoreClusterTest extends OrientEventStoreTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        PerInstanceClusterResolver clusterResolver = new PerInstanceClusterResolver();
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
    }
}
