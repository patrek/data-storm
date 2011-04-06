package datastorm.eventstore.otientdb;

public class SnapshotEventStoreSimpleClusterTest extends SnapshotOrientEventStoreTest {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final SimpleClusterResolver clusterResolver = new SimpleClusterResolver("BigCluster");
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
    }
}
