package datastorm.eventstore.otientdb;

/**
 *  Integration tests that tests {@link OrientEventStore} implementation
 *  of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 *  This test uses {@link PerInstanceClusterResolver}.
 *
 *  @author Andrey Lomakin
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
