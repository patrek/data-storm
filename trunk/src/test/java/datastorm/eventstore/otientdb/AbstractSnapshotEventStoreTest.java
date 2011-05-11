package datastorm.eventstore.otientdb;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.axonframework.domain.DomainEventStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.*;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.agId;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertDomainEventsEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for integration test cases that test {@link OrientEventStore} implementation
 * of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 * <p/>
 * All children of given class should specify only {@link ClusterResolver} or database implementation,
 * but all tests should implemented in given class. This done to test all test cases under different
 * environments.
 *
 * @author Andrey Lomakin
 *         Date: 10.04.11
 */
public abstract class AbstractSnapshotEventStoreTest {
    protected ODatabaseDocumentTx database;
    protected OrientEventStore orientEventStore;
    private boolean oldKeepOpen;

    @Before
    public void setUp() throws Exception {
        oldKeepOpen = OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean();
        OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);

        database = new ODatabaseDocumentTx("local:target/default");
        database.create();
        orientEventStore = new OrientEventStore();
        orientEventStore.setDatabase(database);
    }

    @After
    public void tearDown() throws Exception {
        database.delete();

        OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(oldKeepOpen);
    }

    @Test
    public void testEventSchema() {
        orientEventStore.appendSnapshotEvent("Simple", new SimpleDomainEvent(1, agId("1"), "val"));

        ORecordIteratorClass<ODocument> iteratorClass = database.browseClass(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS,
                false);
        assertTrue(iteratorClass.hasNext());
        final ODocument eventDocument = iteratorClass.next();
        final OClass eventClass = eventDocument.getSchemaClass();

        assertSnapshotEventSchema(eventClass);

        assertEquals(1, eventClass.getClusterIds().length);
    }

    @Test
    public void testStoringWithSnapshot() {
        final List<SimpleDomainEvent> firstDomainEvents = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(firstDomainEvents));

        orientEventStore.appendSnapshotEvent("Aggregate", new SimpleDomainEvent(3, agId("1"), "val"));

        final List<SimpleDomainEvent> secondDomainEvents = createSimpleDomainEvents(new int[]{4, 5},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(secondDomainEvents));

        final SimpleDomainEvent snapshotEvent = new SimpleDomainEvent(6, agId("1"), "val");

        orientEventStore.appendSnapshotEvent("Aggregate", snapshotEvent);

        final List<SimpleDomainEvent> thirdDomainEvents = createSimpleDomainEvents(new int[]{7, 8},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(thirdDomainEvents));

        final DomainEventStream readStream = orientEventStore.readEvents("Aggregate", agId("1"));

        final List<SimpleDomainEvent> resultEvents = new ArrayList<SimpleDomainEvent>();
        resultEvents.add(snapshotEvent);
        resultEvents.addAll(thirdDomainEvents);

        assertDomainEventsEquality(resultEvents, readStream);
    }

    @Test
    public void testSortingWithSnapshot() {
        final List<SimpleDomainEvent> firstDomainEvents = createSimpleDomainEvents(new int[]{1, 8},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(firstDomainEvents));

        orientEventStore.appendSnapshotEvent("Aggregate", new SimpleDomainEvent(3, agId("1"), "val"));

        final List<SimpleDomainEvent> secondDomainEvents = createSimpleDomainEvents(new int[]{4, 7},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(secondDomainEvents));

        final SimpleDomainEvent snapshotEvent = new SimpleDomainEvent(6, agId("1"), "val");

        orientEventStore.appendSnapshotEvent("Aggregate", snapshotEvent);

        final List<SimpleDomainEvent> thirdDomainEvents = createSimpleDomainEvents(new int[]{5, 2},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(thirdDomainEvents));

        final DomainEventStream readStream = orientEventStore.readEvents("Aggregate", agId("1"));

        final List<SimpleDomainEvent> resultEvents = new ArrayList<SimpleDomainEvent>();
        resultEvents.add(snapshotEvent);
        resultEvents.add(secondDomainEvents.get(1));
        resultEvents.add(firstDomainEvents.get(1));

        assertDomainEventsEquality(resultEvents, readStream);
    }

    @Test
    public void testEmptySnapshotListCorrectlyFetched() {
        orientEventStore.appendSnapshotEvent("AggregateOne", new SimpleDomainEvent(1, agId("1"), "val"));

        final List<SimpleDomainEvent> resultEvents = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"2", "2"});
        orientEventStore.appendEvents("AggregateTwo", stream(resultEvents));
        final DomainEventStream readStream = orientEventStore.readEvents("AggregateTwo", agId("2"));
        assertDomainEventsEquality(resultEvents, readStream);
    }

    @Test
    public void testSchemaSaving() {
        orientEventStore.appendSnapshotEvent("Simple", new SimpleDomainEvent(1, agId("1"), "val"));
        database.close();
        database.open("admin", "admin");
        assertTrue(database.getMetadata().getSchema().existsClass(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS));
    }

    @Test
    public void testOldSnapshotsAreRemoved() {
        orientEventStore.setLeaveLastSnapshotOnly(true);
        orientEventStore.appendSnapshotEvent("Simple", new SimpleDomainEvent(1, agId("1"), "val"));
        orientEventStore.appendSnapshotEvent("Simple", new SimpleDomainEvent(2, agId("1"), "val"));

        long snapshotCounts = database.countClass(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS);
        assertEquals(1, snapshotCounts);
    }

    @Test
    public void testOldSnapshotsAreNotRemoved() {
        orientEventStore.setLeaveLastSnapshotOnly(true);
        orientEventStore.appendSnapshotEvent("Simple", new SimpleDomainEvent(1, agId("1"), "val"));
        orientEventStore.appendSnapshotEvent("Simple", new SimpleDomainEvent(2, agId("1"), "val"));

        long snapshotCounts = database.countClass(SnapshotEventEntry.SNAPSHOT_EVENT_CLASS);
        assertEquals(1, snapshotCounts);
    }
}
