package ua.com.datastorm.eventstore.orientdb;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.assertClassHasClusterIds;
import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.assertClusterNames;

/**
 * Integration test case for {@link OrientEventStore}.
 * This test case tests storing and reading events in case when
 * {@link ClusterResolver} is {@link PerInstanceClusterResolver} .
 *
 * @author Andrey Lomakin
 */
public class OrientEventStorePerInstanceClusterTest extends AbstractEventStoreTest {
    private Collection<String> beforeClusters;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        final PerInstanceClusterResolver clusterResolver = new PerInstanceClusterResolver();
        clusterResolver.setDatabase(database);
        orientEventStore.setClusterResolver(clusterResolver);
        beforeClusters = new ArrayList<String>(database.getClusterNames());
    }

    @Override
    @Test
    public void testBasicEventsStoring() throws Exception {
        super.testBasicEventsStoring();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"simple.1"});
        assertClassHasClusterIds(new String[]{"simple.1"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    @Test
    public void testEventsFromDifferentTypesWithSameId() {
        super.testEventsFromDifferentTypesWithSameId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"docone.1", "doctwo.1"});
        assertClassHasClusterIds(new String[]{"docone.1", "doctwo.1"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    @Test
    public void testEventsFromDifferentTypesWithDiffId() {
        super.testEventsFromDifferentTypesWithDiffId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"docone.1", "doctwo.2"});
        assertClassHasClusterIds(new String[]{"docone.1", "doctwo.2"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }

    @Override
    @Test
    public void testEventsWithDiffId() {
        super.testEventsWithDiffId();

        final Collection<String> afterClusters = database.getClusterNames();
        assertClusterNames(beforeClusters, afterClusters, new String[]{"doc.1", "doc.2"});
        assertClassHasClusterIds(new String[]{"doc.1", "doc.2"}, DomainEventEntry.DOMAIN_EVENT_CLASS, database);
    }
}
