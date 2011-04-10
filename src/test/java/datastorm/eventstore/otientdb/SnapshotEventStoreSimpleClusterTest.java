package datastorm.eventstore.otientdb;

/**
 *  Integration tests that tests {@link OrientEventStore} implementation
 *  of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 *  This test uses {@link SimpleClusterResolver}.
 *
 *  @author Andrey Lomakin
 */
public class SnapshotEventStoreSimpleClusterTest extends SnapshotOrientEventStoreTest {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final SimpleClusterResolver clusterResolver = new SimpleClusterResolver("BigCluster");
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
    }
}
