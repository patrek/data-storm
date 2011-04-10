package datastorm.eventstore.otientdb;

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
import java.util.Set;

import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.*;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.agId;
import static datastorm.eventstore.otientdb.OrientEventStoreTestUtils.assertDomainEventsEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base class for integration test cases for {@link OrientEventStore}.
 * This test case tests only  storing and reading events.
 * Snapshots events are not covered.
 *
 * All children of given class should specify only {@link ClusterResolver} or database implementation,
 * but all tests should implemented in given class. This done to test all test cases under different
 * environments.
 *
 * @author Andrey Lomakin
 *         Date: 10.04.11
 */
public abstract class AbstractEventStoreTest {
    protected ODatabaseDocumentTx database;
    protected OrientEventStore orientEventStore;

    @Before
    public void setUp() throws Exception {
        database = new ODatabaseDocumentTx("memory:default");
        database.create();
        orientEventStore = new OrientEventStore();
        orientEventStore.setDatabase(database);
    }

    @After
    public void tearDown() throws Exception {
        database.delete();
    }

    @Test
    public void testEmptyListEventReading() {
        final DomainEventStream eventStream = orientEventStore.readEvents("Doc", agId("1"));
        assertFalse(eventStream.hasNext());
    }

    @Test
    public void testSchemaSaving() {
        final List<SimpleDomainEvent> domainEvents = new ArrayList<SimpleDomainEvent>();
        domainEvents.add(new SimpleDomainEvent(1, agId("1"), "val"));
        orientEventStore.appendEvents("Simple", stream(domainEvents));
        database.close();
        database.open("writer", "writer");
        assertTrue(database.getMetadata().getSchema().existsClass(DomainEventEntry.DOMAIN_EVENT_CLASS));
    }

    @Test
    public void testEventsAppending() {
        final List<SimpleDomainEvent> domainEvents = new ArrayList<SimpleDomainEvent>();
        domainEvents.add(new SimpleDomainEvent(1, agId("1"), "val"));

        orientEventStore.appendEvents("Doc", stream(domainEvents));

        ORecordIteratorClass<ODocument> iteratorClass = database.browseClass(DomainEventEntry.DOMAIN_EVENT_CLASS,
                false);
        assertTrue(iteratorClass.hasNext());
        final ODocument eventDocument = iteratorClass.next();

        final Set<String> fieldNames = eventDocument.fieldNames();
        assertEquals(5, fieldNames.size());

        assertTrue(fieldNames.contains("aggregateIdentifier"));
        assertTrue(fieldNames.contains("sequenceNumber"));
        assertTrue(fieldNames.contains("timestamp"));
        assertTrue(fieldNames.contains("body"));
        assertTrue(fieldNames.contains("aggregateType"));

        assertEquals("Doc", eventDocument.<String>field("aggregateType"));
        assertEquals("1", eventDocument.<String>field("aggregateIdentifier"));
        assertEquals((Long) 1L, eventDocument.<Long>field("sequenceNumber"));
        assertEquals(domainEvents.get(0).getTimestamp().toString(),
                eventDocument.<String>field("timestamp"));

        assertFalse(iteratorClass.hasNext());
    }

    @Test
    public void testEventSchema() {
        final List<SimpleDomainEvent> domainEvents = new ArrayList<SimpleDomainEvent>();
        domainEvents.add(new SimpleDomainEvent(1, agId("1"), "val"));

        orientEventStore.appendEvents("Doc", stream(domainEvents));

        ORecordIteratorClass<ODocument> iteratorClass = database.browseClass(DomainEventEntry.DOMAIN_EVENT_CLASS,
                false);
        assertTrue(iteratorClass.hasNext());
        final ODocument eventDocument = iteratorClass.next();
        final OClass eventClass = eventDocument.getSchemaClass();

        assertDomainEventSchema(eventClass);

        assertEquals(1, eventClass.getClusterIds().length);
    }

    @Test
    public void testBasicEventsStoring() throws Exception {

        final List<SimpleDomainEvent> domainEvents = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"1", "1"});

        orientEventStore.appendEvents("Simple", stream(domainEvents));

        DomainEventStream readEventStream = orientEventStore.readEvents("Simple", agId("1"));

        assertDomainEventsEquality(domainEvents, readEventStream);
    }

    @Test
    public void testEventsSorting() throws Exception {

        final List<SimpleDomainEvent> domainEvents = createSimpleDomainEvents(
                new int[]{3, 1, 5, 9, 2, 4, 6, 8, 7},
                new String[]{"1", "1", "1", "1", "1", "1", "1", "1", "1"}
        );

        orientEventStore.appendEvents("Simple", stream(domainEvents));

        DomainEventStream readEventStream = orientEventStore.readEvents("Simple", agId("1"));

        assertDomainEventsEquality(sortBySequenceNumber(domainEvents), readEventStream);
    }

    @Test
    public void testEventsFromDifferentTypesWithSameId() {
        final List<SimpleDomainEvent> domainEventsDocOne = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"1", "1"});

        final List<SimpleDomainEvent> domainEventsDocTwo = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"1", "1"});

        orientEventStore.appendEvents("DocOne", stream(domainEventsDocOne));
        orientEventStore.appendEvents("DocTwo", stream(domainEventsDocTwo));

        DomainEventStream readEventStreamDocTwo = orientEventStore.readEvents("DocTwo", agId("1"));

        DomainEventStream readEventStreamDocOne = orientEventStore.readEvents("DocOne", agId("1"));


        assertDomainEventsEquality(domainEventsDocOne, readEventStreamDocOne);
        assertDomainEventsEquality(domainEventsDocTwo, readEventStreamDocTwo);
    }

    @Test
    public void testEventsFromDifferentTypesWithDiffId() {
        final List<SimpleDomainEvent> domainEventsDocOne = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"1", "1"});
        final List<SimpleDomainEvent> domainEventsDocTwo = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"2", "2"});

        orientEventStore.appendEvents("DocOne", stream(domainEventsDocOne));
        orientEventStore.appendEvents("DocTwo", stream(domainEventsDocTwo));

        DomainEventStream readEventStreamDocTwo = orientEventStore.readEvents("DocTwo", agId("2"));

        DomainEventStream readEventStreamDocOne = orientEventStore.readEvents("DocOne", agId("1"));


        assertDomainEventsEquality(domainEventsDocOne, readEventStreamDocOne);
        assertDomainEventsEquality(domainEventsDocTwo, readEventStreamDocTwo);
    }

    @Test
    public void testEventsWithDiffId() {
        final List<SimpleDomainEvent> domainEventsDocOne = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"1", "1"});
        final List<SimpleDomainEvent> domainEventsDocTwo = createSimpleDomainEvents(new int[]{1, 2},
                new String[]{"2", "2"});

        orientEventStore.appendEvents("Doc", stream(domainEventsDocOne));
        orientEventStore.appendEvents("Doc", stream(domainEventsDocTwo));

        DomainEventStream readEventStreamDocTwo = orientEventStore.readEvents("Doc", agId("2"));

        DomainEventStream readEventStreamDocOne = orientEventStore.readEvents("Doc", agId("1"));

        assertDomainEventsEquality(domainEventsDocOne, readEventStreamDocOne);
        assertDomainEventsEquality(domainEventsDocTwo, readEventStreamDocTwo);
    }
}
