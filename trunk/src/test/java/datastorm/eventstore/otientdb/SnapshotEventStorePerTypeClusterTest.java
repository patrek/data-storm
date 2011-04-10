package datastorm.eventstore.otientdb;

/**
 *  Integration tests that tests {@link OrientEventStore} implementation
 *  of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 *  This test uses {@link PerTypeClusterResolver}.
 *
 *  @author Andrey Lomakin
 */
public class SnapshotEventStorePerTypeClusterTest extends SnapshotOrientEventStoreTest{
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final PerTypeClusterResolver clusterResolver = new PerTypeClusterResolver();
         clusterResolver.setDatabase(database);
         orientEventStore.setClusterResolver(clusterResolver);
    }
}
