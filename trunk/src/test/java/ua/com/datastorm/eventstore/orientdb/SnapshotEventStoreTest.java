package ua.com.datastorm.eventstore.orientdb;

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

import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.*;
import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.agId;
import static ua.com.datastorm.eventstore.orientdb.OrientEventStoreTestUtils.assertDomainEventsEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for integration test cases that test {@link OrientEventStore} implementation
 * of {@link org.axonframework.eventstore.SnapshotEventStore} interface.
 * <p/>
 *
 * @author Andrey Lomakin
 *         Date: 10.04.11
 */
public class SnapshotEventStoreTest {
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

        orientEventStore.appendSnapshotEvent("Aggregate", new SimpleDomainEvent(2, agId("1"), "val"));

        final List<SimpleDomainEvent> secondDomainEvents = createSimpleDomainEvents(new int[]{3, 4},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(secondDomainEvents));

        final SimpleDomainEvent snapshotEvent = new SimpleDomainEvent(4, agId("1"), "val");

        orientEventStore.appendSnapshotEvent("Aggregate", snapshotEvent);

        final List<SimpleDomainEvent> thirdDomainEvents = createSimpleDomainEvents(new int[]{5, 6},
                new String[]{"1", "1"});
        orientEventStore.appendEvents("Aggregate", stream(thirdDomainEvents));

        final DomainEventStream readStream = orientEventStore.readEvents("Aggregate", agId("1"));

        final List<SimpleDomainEvent> resultEvents = new ArrayList<SimpleDomainEvent>();
        resultEvents.add(snapshotEvent);
        resultEvents.addAll(thirdDomainEvents);

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
