package datastorm.eventstore.otientdb;

import java.util.ArrayList;
import java.util.Collection;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClassHasClusterIds;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClusterNames;

/**
 * Integration tests that tests {@link OrientEventStore} implementation
 * of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 * This test uses {@link PerTypeClusterResolver}.
 *
 * @author Andrey Lomakin
 */
public class SnapshotEventStorePerTypeClusterTest extends AbstractSnapshotEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final PerTypeClusterResolver clusterResolver = new PerTypeClusterResolver();
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);

        beforeClusters = new ArrayList<String>(database.getClusterNames());
    }

    @Override
    public void testStoringWithSnapshot() {
        super.testStoringWithSnapshot();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"aggregate"});
        assertClassHasClusterIds(new String[]{"aggregate"}, SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{"aggregate"}, SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEmptySnapshotListCorrectlyFetched() {
        super.testEmptySnapshotListCorrectlyFetched();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"aggregateone", "aggregatetwo"});
        assertClassHasClusterIds(new String[]{"aggregateone"},
                SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{"aggregateone", "aggregatetwo"},
                SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);

    }
}
