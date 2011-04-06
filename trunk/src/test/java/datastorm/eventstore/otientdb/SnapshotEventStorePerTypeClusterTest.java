package datastorm.eventstore.otientdb;

public class SnapshotEventStorePerTypeClusterTest extends SnapshotOrientEventStoreTest{
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final PerTypeClusterResolver clusterResolver = new PerTypeClusterResolver();
         clusterResolver.setDatabase(database);
         orientEventStore.setClusterResolver(clusterResolver);
    }
}
