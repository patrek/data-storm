package datastorm.eventstore.otientdb;

/**
 *
 */
public class SnapshotEventStorePerInstanceClusterTest extends SnapshotOrientEventStoreTest {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final PerInstanceClusterResolver clusterResolver = new PerInstanceClusterResolver();
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
    }
}
