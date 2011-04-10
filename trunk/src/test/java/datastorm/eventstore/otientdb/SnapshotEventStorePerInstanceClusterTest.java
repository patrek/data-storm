package datastorm.eventstore.otientdb;

import java.util.Collection;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClassHasClusterIds;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClusterNames;

/**
 * Integration tests that tests {@link OrientEventStore} implementation
 * of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 * This test uses {@link PerInstanceClusterResolver}.
 *
 * @author Andrey Lomakin
 */
public class SnapshotEventStorePerInstanceClusterTest extends AbstractSnapshotEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final PerInstanceClusterResolver clusterResolver = new PerInstanceClusterResolver();
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);

        beforeClusters = database.getClusterNames();
    }

    @Override
    public void testStoringWithSnapshot() {
        super.testStoringWithSnapshot();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"aggregate.1"});
        assertClassHasClusterIds(new String[]{"aggregate.1"}, SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{"aggregate.1"}, SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEmptySnapshotListCorrectlyFetched() {
        super.testEmptySnapshotListCorrectlyFetched();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"aggregateone.1", "aggregatetwo.2"});
        assertClassHasClusterIds(new String[]{"aggregateone.1"},
                SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{"aggregateone.1", "aggregatetwo.2"},
                SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);
    }
}
