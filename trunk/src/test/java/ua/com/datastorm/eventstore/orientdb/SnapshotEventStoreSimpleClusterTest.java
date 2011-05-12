package ua.com.datastorm.eventstore.orientdb;

import java.util.ArrayList;
import java.util.Collection;

import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.assertClassHasClusterIds;
import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.assertClusterNames;

/**
 * Integration tests that tests {@link OrientEventStore} implementation
 * of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 * This test uses {@link SimpleClusterResolver}.
 *
 * @author Andrey Lomakin
 */
public class SnapshotEventStoreSimpleClusterTest extends AbstractSnapshotEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final SimpleClusterResolver clusterResolver = new SimpleClusterResolver("BigCluster");
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);

        beforeClusters = new ArrayList<String>(database.getClusterNames());
    }

    @Override
    public void testStoringWithSnapshot() {
        super.testStoringWithSnapshot();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"bigcluster"});
        assertClassHasClusterIds(new String[]{"bigcluster"}, SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{"bigcluster"}, SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEmptySnapshotListCorrectlyFetched() {
        super.testEmptySnapshotListCorrectlyFetched();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"bigcluster"});
        assertClassHasClusterIds(new String[]{"bigcluster"}, SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{"bigcluster"}, SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);
    }
}
