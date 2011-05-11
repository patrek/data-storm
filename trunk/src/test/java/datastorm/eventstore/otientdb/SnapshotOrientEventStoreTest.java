package datastorm.eventstore.otientdb;

import java.util.ArrayList;
import java.util.Collection;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.*;

/**
 * Integration tests that tests {@link OrientEventStore} implementation
 * of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 * This test does not use cluster resolvers.
 *
 * @author Andrey Lomakin
 */
public class SnapshotOrientEventStoreTest extends AbstractSnapshotEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        beforeClusters = new ArrayList<String>(database.getClusterNames());
    }

    @Override
    public void testStoringWithSnapshot() {
        super.testStoringWithSnapshot();

        final String defaultClusterName = database.getClusterNameById(database.getDefaultClusterId());
        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{});
        assertClassHasClusterIds(new String[]{defaultClusterName}, SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{defaultClusterName}, SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEmptySnapshotListCorrectlyFetched() {
        super.testEmptySnapshotListCorrectlyFetched();

        final String defaultClusterName = database.getClusterNameById(database.getDefaultClusterId());
        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{});
        assertClassHasClusterIds(new String[]{defaultClusterName}, SnapshotEventEntry.SNAPSHOT_EVENT_CLASS, database);
        assertClassHasClusterIds(new String[]{defaultClusterName}, SnapshotEventEntry.DOMAIN_EVENT_CLASS, database);
    }
}
