package ua.com.datastorm.eventstore.orientdb;

import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;

import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.*;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests only  storing and reading events for default cluster.
 * Snapshots events are not covered.
 *
 * @author EniSh
 */
public class OrientEventStoreTest extends AbstractEventStoreTest {

    private Collection<String> beforeClusters;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        beforeClusters = new ArrayList<String>(database.getClusterNames());
    }


    @Override
    public void testBasicEventsStoring() throws Exception {
        super.testBasicEventsStoring();

        final String defaultClusterName = database.getClusterNameById(database.getDefaultClusterId());
        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{});
        assertClassHasClusterIds(new String[]{defaultClusterName},
                DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEventsFromDifferentTypesWithDiffId() {
        super.testEventsFromDifferentTypesWithDiffId();

        final String defaultClusterName = database.getClusterNameById(database.getDefaultClusterId());
        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{});
        assertClassHasClusterIds(new String[]{defaultClusterName},
                DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEventsFromDifferentTypesWithSameId() {
        super.testEventsFromDifferentTypesWithSameId();

        final String defaultClusterName = database.getClusterNameById(database.getDefaultClusterId());
        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{});
        assertClassHasClusterIds(new String[]{defaultClusterName},
                DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    public void testEventsWithDiffId() {
        super.testEventsWithDiffId();

        final String defaultClusterName = database.getClusterNameById(database.getDefaultClusterId());
        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{});
        assertClassHasClusterIds(new String[]{defaultClusterName},
                DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }
}
