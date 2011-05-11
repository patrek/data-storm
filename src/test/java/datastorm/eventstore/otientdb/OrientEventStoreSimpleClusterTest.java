package datastorm.eventstore.otientdb;

import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClassHasClusterIds;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertClusterNames;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests storing and reading events in case when
 * {@link ClusterResolver} is {@link PerTypeClusterResolver} .
 *
 * @author Andrey Lomakin
 */
public class OrientEventStoreSimpleClusterTest extends AbstractEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final SimpleClusterResolver clusterResolver = new SimpleClusterResolver("BigCluster");
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
        beforeClusters = new ArrayList<String>(database.getClusterNames());
    }

    @Override
    public void testBasicEventsStoring() throws Exception {
        super.testBasicEventsStoring();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"bigcluster"});
        assertClassHasClusterIds(new String[]{"bigcluster"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEventsFromDifferentTypesWithSameId() {
        super.testEventsFromDifferentTypesWithSameId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"bigcluster"});
        assertClassHasClusterIds(new String[]{"bigcluster"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEventsFromDifferentTypesWithDiffId() {
        super.testEventsFromDifferentTypesWithDiffId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"bigcluster"});
        assertClassHasClusterIds(new String[]{"bigcluster"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEventsWithDiffId() {
        super.testEventsWithDiffId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"bigcluster"});
        assertClassHasClusterIds(new String[]{"bigcluster"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }
}
